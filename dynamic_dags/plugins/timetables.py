from datetime import timedelta
from typing import Any, Optional

from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction
from airflow.timetables.trigger import CronTriggerTimetable


class SlidingWindowTimetable(CronTriggerTimetable):
    """
    A custom Airflow timetable that schedules DAG runs based on a cron expression
    but creates data intervals that span a configurable processing window.

    Unlike the default behavior where data intervals match the cron schedule period
    (e.g., 1-hour intervals for hourly crons), this timetable creates data intervals
    that represent the actual time range being processed by the DAG tasks.

    The timetable supports an optional jitter_minutes parameter that delays task
    execution (run_after) without affecting the data intervals. This allows load
    spreading while maintaining consistent data window boundaries.

    Example:
        - Cron schedule: "0 * * * *" (hourly)
        - Processing window: 72 hours
        - Processing lag: 30 minutes
        - Jitter: 7 minutes

        For a DAG run scheduled at 15:00:
        - run_after: 15:07 (15:00 + 7 min jitter - when the DAG actually executes)
        - data_interval_end: 14:30 (15:00 - 30 min lag, NOT affected by jitter)
        - data_interval_start: 14:30 three days ago (72 hours before end)

    This makes the Airflow UI correctly display what data is actually being processed,
    rather than just showing the scheduling interval, while still spreading execution
    load across time.
    """

    def __init__(
        self,
        cron: str,
        processing_window_hours: float,
        processing_window_lag: int,
        jitter_minutes: float = 0,
    ):
        # Initialize the parent with the cron schedule
        super().__init__(cron=cron, timezone="UTC")
        self.cron = cron
        self.processing_window_hours = processing_window_hours
        self.processing_window_lag = processing_window_lag
        self.jitter_minutes = jitter_minutes

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        # Somehow this function does not provide the previous trigger time, so
        # infer it based on the last data interval and lag
        # Note that the interval start is not used by super().next_dagrun_info() below
        previous_data_interval = (
            DataInterval(
                start=last_automated_data_interval.start,
                end=last_automated_data_interval.end
                + timedelta(minutes=self.processing_window_lag),
            )
            if last_automated_data_interval
            else None
        )

        # Get the standard DAG run info from parent class
        parent_info = super().next_dagrun_info(
            last_automated_data_interval=previous_data_interval,
            restriction=restriction,
        )

        if parent_info is None:
            return None

        # Keep the scheduled run_after time for data interval calculation
        # This ensures data intervals align to cron boundaries, not jittered times
        scheduled_run_after = parent_info.run_after

        # Calculate custom data interval with processing window and lag
        # These are based on the scheduled time, NOT the jittered execution time
        data_interval_end = scheduled_run_after - timedelta(
            minutes=self.processing_window_lag
        )
        data_interval_start = data_interval_end - timedelta(
            hours=self.processing_window_hours
        )

        # Apply jitter to run_after (execution time) AFTER calculating data intervals
        # This delays when the task executes but keeps data windows aligned
        actual_run_after = scheduled_run_after + timedelta(minutes=self.jitter_minutes)

        return DagRunInfo(
            run_after=actual_run_after,
            data_interval=DataInterval(data_interval_start, data_interval_end),
        )

    def serialize(self) -> dict[str, Any]:
        return {
            "cron": self.cron,
            "processing_window_hours": self.processing_window_hours,
            "processing_window_lag": self.processing_window_lag,
            "jitter_minutes": self.jitter_minutes,
        }

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "SlidingWindowTimetable":
        return cls(
            cron=data["cron"],
            processing_window_hours=data["processing_window_hours"],
            processing_window_lag=data["processing_window_lag"],
            jitter_minutes=data.get(
                "jitter_minutes", 0
            ),  # Default to 0 for backwards compatibility
        )
