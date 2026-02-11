import json
import logging
from datetime import datetime, timedelta
import os
from typing import List, Optional, NamedTuple, cast

from airflow.providers.cncf.kubernetes.operators.pod import (
    KubernetesPodOperator,
)
from airflow.sdk import BaseOperator, Context, DAG, Param
from airflow.sdk.definitions.param import ParamsDict
from airflow.sdk.definitions.decorators import task, dag

from kubernetes.client.models import V1ResourceRequirements

import boto3
import psycopg2 as psycopg

from utils.calculate_jitter_amount import calculate_jitter_amount
from utils.even_batches import (
    even_batches,
)
from utils.generate_batch_tasks import (
    generate_batch_tasks,
)

from plugins.timetables import SlidingWindowTimetable

task_logger = logging.getLogger("airflow.task")

CLIENT_PARAM = "Client"
ALL_CLIENTS_OPTION = "All Clients"
PREFLIGHT_TASK_RETRIES = (
    5  # Extra retries for fetch_and_batch_ids and create_k8s_task_definition
)


class BatchInfo(NamedTuple):
    size: Optional[int] = None
    query: str = "SELECT id FROM sites.site WHERE deleted = false"


class PodResources(NamedTuple):
    """
    :param cpu_request: In milliCPU. E.g. 1000 = 1 CPU
    :param ram_request: In Mi.
    """

    cpu_request: int = 1000
    ram_request: int = 4000


class _SiteIdsKubernetesPodOperator(KubernetesPodOperator):
    """
    KubernetesPodOperator that exposes site_ids in the Airflow UI by adding
    them to template_fields. Used when building mapped K8s tasks per batch.
    """

    template_fields = (
        *KubernetesPodOperator.template_fields,
        "site_ids_display",
    )

    def __init__(self, *args, **kwargs):
        op_kwargs = kwargs.pop("op_kwargs", {})
        self.site_ids_display = op_kwargs.get("site_ids", [])
        super().__init__(*args, **kwargs)


class AirFlowDagCreator:
    def __init__(
        self,
        model: str,
        cron_schedule: str,
        dag_tags: list[str],
        start_ts: Optional[datetime] = None,
        end_ts: Optional[datetime] = None,
        env_vars: dict[str, str] = dict(),
        processing_window_hours: float = 24,
        processing_window_lag: int = 0,
        batch_info: Optional[BatchInfo] = None,
        k8s_pod_resources: PodResources = PodResources(),
        task_retries: int = 2,
        retry_delay_seconds: int = 300,
        retry_exponential_backoff: bool = True,
        max_active_tasks: int = 64,
        startup_timeout_seconds: int = 40 * 60,
        dry_run: bool = False,
        catchup: bool = False,
        enable_schedule_jitter: bool = True,
        schedule_jitter_minutes: int = 15,
    ):
        self.model = model

        self.dag_name = self.model

        # Keep the original cron schedule (no modification)
        # Jitter will be applied in the SlidingWindowTimetable instead
        self.cron_schedule = cron_schedule

        # Calculate jitter amount if enabled (for passing to timetable)
        if enable_schedule_jitter:
            self.jitter_amount = calculate_jitter_amount(
                base_cron=self.cron_schedule,
                dag_name=self.dag_name,
                jitter_minutes=schedule_jitter_minutes,
            )
        else:
            self.jitter_amount = 0.0

        self.start_ts = start_ts
        self.dag_tags = set(dag_tags)
        self.end_ts = end_ts

        self.env_vars = {
            "EVENTS_TOPIC": os.environ["KAFKA_EVENTS_TOPIC"],
            "KAFKA_AWS_SECRET_NAME": os.environ["LIONFISH_KAFKA_ARN"],
            "KAFKA_SCHEMA_REGISTRY_SECRET": os.environ["SCHEMA_REGISTRY_SECRET_NAME"],
            "APP_DB_SECRET_ARN": os.environ["LIONFISH_APP_RDS_ARN"],
            "DATA_BUCKET_NAME": os.environ["DATA_BUCKET_NAME"],
            "AUTH_TOKEN_PRIVATE_KEY": os.environ["AUTH_TOKEN_PRIVATE_KEY"],
            "SITE_EVENTS_SERVICE_URL": os.environ["SITE_EVENTS_SERVICE_URL"],
            "TIMESCALE_DB_SECRET_ARN": os.environ["LIONFISH_TIMESCALE_ARN"],
            "METRICS_TOPIC": os.environ["KAFKA_METRICS_TOPIC"],
            "LATE_METRICS_TOPIC": os.environ["KAFKA_LATE_METRICS_TOPIC"],
            "DERIVED_METRICS_TOPIC": os.environ["KAFKA_DERIVED_METRICS_TOPIC"],
            "HALF_HOUR_AGGREGATION_TOPIC": os.environ[
                "KAFKA_HALF_HOUR_AGGREGATION_TOPIC"
            ],
            "DAILY_AGGREGATION_TOPIC": os.environ["KAFKA_DAILY_AGGREGATION_TOPIC"],
            "ATC_CLIENT_ID": os.environ["ATC_CLIENT_ID"],
        } | env_vars

        self.service_account_name = os.environ["SERVICE_ACCOUNT_NAME"]
        self.processing_window_hours = processing_window_hours
        self.processing_window_lag = processing_window_lag
        self.batch_info = batch_info
        self.k8s_pod_resources = k8s_pod_resources
        self.task_retries = task_retries
        self.retry_delay_seconds = retry_delay_seconds
        self.retry_exponential_backoff = retry_exponential_backoff
        self.startup_timeout_seconds = startup_timeout_seconds
        self.dry_run = dry_run
        self.max_active_tasks = max_active_tasks
        self.catchup = catchup

        self.base_image_args = [
            "--model",
            self.model,
            "--window_start_timestamp",
            "{{ macros.ds_format(data_interval_start, '%Y-%m-%d %H:%M:%S%z', '%Y-%m-%dT%H:%M:%S') }}",
            "--window_end_timestamp",
            "{{ macros.ds_format(data_interval_end, '%Y-%m-%d %H:%M:%S%z', '%Y-%m-%dT%H:%M:%S') }}",
        ]

        if self.dry_run:
            self.base_image_args.append("--dry_run")

    def render_query_template(self, template_string: str, context: Context) -> str:
        """
        Renders a Jinja template string using the provided context.

        :param template_string: The Jinja template as a string.
        :param context: The Airflow context containing variables for rendering.
        :return: The rendered string.
        """
        # Create a dummy BaseOperator to access the render_template method
        dummy_operator = BaseOperator(task_id="dummy_task")
        # Render the template string
        rendered_string = dummy_operator.render_template(
            content=template_string, context=context
        )
        return rendered_string

    @classmethod
    def _get_db_connection(cls):
        """Get a database connection using AWS Secrets Manager credentials."""
        client = boto3.client("secretsmanager")
        secret_value = client.get_secret_value(
            SecretId=os.environ["LIONFISH_APP_RDS_ARN"]
        )
        db_creds = json.loads(secret_value["SecretString"])

        return psycopg.connect(
            dbname=db_creds["database"],
            user=db_creds["username"],
            password=db_creds["password"],
            host=db_creds["url"],
            port=5432,
            application_name="airflow_dag_maker",
        )

    @classmethod
    def fetch_batch_ids(cls, query: str) -> List[int] | List[str]:
        connection = cls._get_db_connection()

        try:
            with connection.cursor() as cursor:
                task_logger.info(f"Database query: {query}")

                cursor.execute(query)
                result = cursor.fetchall()
                return [row[0] for row in result]

        finally:
            connection.close()

    @classmethod
    def fetch_active_clients(cls) -> list[tuple[int, str]]:
        """
        Fetch active clients from the database for the dropdown selector.

        Returns:
            List of tuples (id, name) for active clients, sorted by name.
        """
        connection = cls._get_db_connection()

        try:
            with connection.cursor() as cursor:
                query = "SELECT id, name FROM public.clients WHERE is_active = true ORDER BY name"
                cursor.execute(query)
                result = cursor.fetchall()
                return [(row[0], row[1]) for row in result]
        finally:
            connection.close()

    @classmethod
    def build_client_enum_values(cls) -> list[str]:
        """
        Build enum values for client dropdown.

        Returns list like: ["All Clients", "1: Acme Corp", "2: Beta Inc", ...]
        The first option allows users to run for all clients.
        """
        try:
            clients = cls.fetch_active_clients()
            # Format: "id: name" so we can parse the ID later
            enum_values: list[str] = [ALL_CLIENTS_OPTION]
            enum_values.extend([f"{client_id}: {name}" for client_id, name in clients])
            return enum_values
        except Exception as e:
            # If we can't fetch clients (e.g., during local development),
            # fall back to allowing any value
            task_logger.warning(f"Failed to fetch clients for dropdown: {e}")
            return [ALL_CLIENTS_OPTION]

    @staticmethod
    def append_client_filter(query: str, client_id: int | str) -> str:
        """
        Append a client_id filter to an existing SQL query.

        Simply adds 'AND client_id = {client_id}' to the query. This works correctly
        even with existing client_id conditions (e.g., ATC exclusion) because:
        - 'client_id != 163 AND client_id = 1' returns client 1's sites (1 != 163 is true)
        - 'client_id != 163 AND client_id = 163' returns nothing (correctly excludes ATC)

        Args:
            query: The original SQL query
            client_id: The client ID to filter for (int or string convertible to int)

        Returns:
            Modified query with additional client_id filter

        Raises:
            ValueError: If client_id cannot be converted to an integer
        """
        try:
            client_id_int = int(client_id)
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"client_id must be an integer or string convertible to integer, "
                f"got {type(client_id).__name__}: {client_id}"
            ) from e
        return f"{query} AND client_id = {client_id_int}"

    def create_dag(self):
        timetable = SlidingWindowTimetable(
            cron=self.cron_schedule,
            processing_window_hours=self.processing_window_hours,
            processing_window_lag=self.processing_window_lag,
            jitter_minutes=self.jitter_amount,
        )

        # Build params conditionally based on DAG configuration
        dag_params: dict[str, Param] = {}

        # Only show client filter param when batch_info is configured
        if self.batch_info:
            dag_params[CLIENT_PARAM] = Param(
                default=ALL_CLIENTS_OPTION,
                enum=self.build_client_enum_values(),
                description=(
                    "Optional client to filter sites. "
                    "If not specified, runs for all clients (except ATC). "
                    "Use this parameter during backfills to process a specific client."
                ),
            )

        @dag(
            dag_id=self.dag_name,
            schedule=timetable,
            start_date=self.start_ts,
            end_date=self.end_ts,
            tags=self.dag_tags,
            max_active_tasks=self.max_active_tasks,
            catchup=self.catchup,
            params=ParamsDict(dag_params),
        )
        def build_dag():
            @task(
                retries=PREFLIGHT_TASK_RETRIES,
                retry_delay=timedelta(seconds=self.retry_delay_seconds),
            )
            def fetch_and_batch_ids(
                **context,
            ) -> list[tuple[int, list[int]]]:
                return _fetch_and_batch_ids_logic(self=self, context=context)

            @task(
                retries=PREFLIGHT_TASK_RETRIES,
                retry_delay=timedelta(seconds=self.retry_delay_seconds),
            )
            def create_k8s_task_definition(**kwargs) -> dict:
                return _create_k8s_task_definition_logic(self=self, kwargs=kwargs)

            k8s_resources = V1ResourceRequirements(
                requests={
                    "cpu": f"{self.k8s_pod_resources.cpu_request}m",
                    "memory": f"{self.k8s_pod_resources.ram_request}Mi",
                }
            )

            # Use the output of the preflight task to get batch IDs and create K8s task definitions
            batches = fetch_and_batch_ids()
            # For each batch, run a task that returns a task definition
            k8s_task_definitions = create_k8s_task_definition.expand(task_data=batches)
            # Use the task definitions to create KubernetesPodOperator tasks dynamically
            _SiteIdsKubernetesPodOperator.partial(
                task_id="mapped_k8s_tasks",
                container_resources=k8s_resources,
                retries=self.task_retries,
                retry_delay=timedelta(seconds=self.retry_delay_seconds),
                retry_exponential_backoff=self.retry_exponential_backoff,
                on_finish_action="delete_pod",
                reattach_on_restart=True,
            ).expand_kwargs(k8s_task_definitions)

        return build_dag()


def _fetch_and_batch_ids_logic(
    self: AirFlowDagCreator, context: Context
) -> list[tuple[int, list[int]]]:
    """Fetch and batch IDs for sites and return task mappings."""
    if not self.batch_info:
        # There is 1 task, indexed at 0, with no batched IDs.
        return [(0, [])]

    # Get optional client from DAG run params or conf (used for backfills)
    # - params: populated when triggering via Airflow UI
    # - dag_run.conf: populated when using CLI with --dag-run-conf
    context = cast(Context, context)
    params = context.get("params", {})
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    task_logger.info(f"DAG params debug: params={params}, conf={conf}")

    # Get the client selection (format: "id: name" or None)
    # Check params first (UI), then conf (CLI --dag-run-conf)
    client_selection = params.get(CLIENT_PARAM) or conf.get(CLIENT_PARAM)
    task_logger.info(
        f"Client selection: {client_selection!r} (type={type(client_selection).__name__})"
    )

    site_batches = None

    # Fetch site batches if configured
    if batch_info_data := self.batch_info:
        query = batch_info_data.query

        rendered_query = self.render_query_template(
            template_string=query,
            context=context,
        )
        raw_ids = cast(List[int], self.fetch_batch_ids(query=rendered_query))
        if not raw_ids:
            task_logger.warning("No site IDs fetched from database for batching.")
            return []

        if size := batch_info_data.size:
            site_batches = even_batches(raw_ids, size=size)
        else:
            site_batches = [raw_ids]

        task_logger.info(f"Created {len(site_batches)} site batches")

    # Generate tasks based on configuration
    data = generate_batch_tasks(
        site_batches=site_batches,
    )

    return data


def _create_k8s_task_definition_logic(self: AirFlowDagCreator, kwargs: dict) -> dict:
    """Build KubernetesPodOperator kwargs for a single batch."""
    task_data: tuple[int, list[int]] | None = kwargs.get("task_data")

    if not task_data:
        task_logger.error("task_data not found in kwargs")
        return {}

    task_index, site_ids = task_data

    # Build strings for site_ids
    site_ids_str = ",".join([str(i) for i in site_ids]) if site_ids else ""

    # Build env vars with batch IDs
    task_env_vars = self.env_vars.copy()
    if site_ids_str:
        task_env_vars["SITE_IDS"] = site_ids_str

    # Pass client info to pod if specified in DAG params (for logging/debugging)
    client_selection = kwargs.get("params", {}).get(CLIENT_PARAM)
    if client_selection is not None:
        # Pass the full selection (e.g., "123: Client Name") for better debugging
        task_env_vars["CLIENT_FILTER"] = str(client_selection)

    return {
        "name": f"{self.dag_name}_task_{task_index}",
        "task_id": f"{self.dag_name}_k8s_task_{task_index}",
        "namespace": os.environ["KUBERNETES_NAMESPACE"],
        "service_account_name": self.service_account_name,
        "image": os.environ["LIONFISH_DOCKER_IMAGE"],
        "image_pull_policy": "Always",
        "cmds": ["python", "-m", "lionfish"],
        "arguments": self.base_image_args,
        "node_selector": {"powerx.ai/workload": "general-x64"},
        "annotations": {
            "cluster-autoscaler.kubernetes.io/safe-to-evict": "false",
        },
        "labels": {
            "app": "lionfish",
        },
        "env_vars": task_env_vars,
        "startup_timeout_seconds": self.startup_timeout_seconds,
        # Store as op_kwargs for visibility in Airflow UI Details tab
        "op_kwargs": {
            "site_ids": site_ids,
            "task_index": task_index,
        },
    }
