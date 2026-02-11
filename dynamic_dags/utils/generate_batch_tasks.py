import logging

logger = logging.getLogger("airflow.task")


def generate_batch_tasks(
    site_batches: list[list[int]] | None = None,
) -> list[tuple[int, list[int]]]:
    """
    Generate task configurations from site batches.

    This function takes batches and returns task configurations without
    mutating its inputs. It emits log messages for observability.

    Args:
        site_batches: Optional list of site ID batches

    Returns:
        List of tuples: (task_index, site_ids)
        - task_index: Sequential task number
        - site_ids: List of site IDs for this task (empty if no site batching)

    Behavior:
        - If only site_batches: One task per site batch
        - If no batches: Single task with empty site_ids

    Examples:
        >>> generate_batch_tasks(site_batches=[[1, 2], [3, 4]])
        [(0, [1, 2]),
         (1, [3, 4])]
        >>> generate_batch_tasks(
        ...     site_batches=[[1, 2]],
        ... )
        [(0, [1, 2])]
    """
    if site_batches:
        # Only site batching
        logger.info(f"Created {len(site_batches)} site-batched tasks")
        return [(e, site_batch) for e, site_batch in enumerate(site_batches)]

    else:
        # No batching - single task with no batch IDs
        logger.info("No batching configured - creating single task")
        return [(0, [])]
