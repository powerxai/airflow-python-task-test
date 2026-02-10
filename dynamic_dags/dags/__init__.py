import pendulum
from ..lib.dag_maker import AirFlowDagCreator, BatchInfo


model = AirFlowDagCreator(
    model="BatteryCapacityChange",
    start_ts=pendulum.datetime(2025, 3, 20, tz="UTC"),
    dag_tags=["battery", "event"],
    task_retries=2,
    cron_schedule="10 * * * *",
    batch_info=BatchInfo(size=4000),
    processing_window_lag=70,
    processing_window_hours=6,
)

model.create_dag()
