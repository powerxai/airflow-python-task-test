from airflow.plugins_manager import AirflowPlugin

from plugins.timetables import SlidingWindowTimetable


class SlidingWindowTimetablePlugin(AirflowPlugin):
    name = "sliding_window_timetable"
    timetables = [SlidingWindowTimetable]
