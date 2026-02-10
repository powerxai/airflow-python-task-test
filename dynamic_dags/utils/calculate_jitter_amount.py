import hashlib
import logging

logger = logging.getLogger("airflow.task")

def calculate_jitter_amount(
    base_cron: str,
    dag_name: str,
    jitter_minutes: int = 15,
) -> float:
    """
    Generate a jitter amount in minutes to spread load across time.

    This function takes a base cron schedule and applies a deterministic jitter
    based on the DAG name, so the same DAG always gets the same schedule but
    different DAGs get different schedules.

    Args:
        base_cron: Base cron expression (e.g., "0 * * * *" for hourly)
        dag_name: Name of the DAG (used to generate deterministic jitter)
        jitter_minutes: Maximum jitter in minutes (default 15, must be non-negative)

    Returns:
        Number of minutes to add as jitter (0.0 to jitter_minutes, exclusive), or 0.0 if jitter
        cannot be applied (invalid cron format, interval-based schedule, or non-integer
        minute field). The jitter is calculated as a percentage-based offset for better
        distribution across the time window.

    Examples:
        >>> calculate_jitter_amount("0 * * * *", "BatteryModel", 15)
        7.35  # Returns a deterministic jitter between 0.0 and 15.0 minutes

        >>> calculate_jitter_amount("5 0 * * *", "FuelModel", 30)
        23.7  # Returns a deterministic jitter between 0.0 and 30.0 minutes

        >>> calculate_jitter_amount("*/15 * * * *", "BatteryModel", 15)
        0.0  # Returns 0.0 for interval-based schedules (no jitter applied)

    Note:
        - Requires a valid 5-field cron expression with an integer minute field
        - The jitter is deterministic based on the dag_name hash (same DAG always gets same jitter)
        - Interval-based minute fields (e.g., "*/15 * * * *") are not jittered and return 0.0
        - Fixed-time schedules (e.g., "0 * * * *" or "5 0 * * *") are jittered
        - Invalid cron formats or non-integer minute fields return 0.0
        - Uses modulo 100 on hash to get percentage for finer-grained distribution
    """
    if jitter_minutes < 0:
        raise ValueError("jitter_minutes must be non-negative")

    parts = base_cron.split()

    if len(parts) < 5:
        logger.warning(f"Invalid cron format for jittering: {base_cron}")
        return 0.0

    minute_field = parts[0]
    if minute_field.startswith("*"):
        logger.info(f"Cron has interval minute field, skipping jitter: {base_cron}")
        return 0.0

    # Validate minute field is a simple integer
    try:
        int(minute_field)
    except ValueError:
        logger.warning(
            f"Cron minute field has an unexpected format, skipping jitter: {base_cron}"
        )
        return 0.0

    # Generate deterministic jitter based on DAG name
    # Use modulo 100 to get a percentage (0-99), then scale to jitter_minutes
    hash_value = int(hashlib.md5(dag_name.encode()).hexdigest(), 16)
    percentage = (hash_value % 100) / 100.0  # 0.0 to 0.99
    jitter_offset = percentage * jitter_minutes

    return jitter_offset
