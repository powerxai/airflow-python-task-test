import logging

logger = logging.getLogger("airflow.task")


def generate_batch_tasks(
    site_batches: list[list[int]] | None = None,
    metric_batches: list[list[str]] | None = None,
) -> list[tuple[int, list[int], list[str]]]:
    """
    Generate task configurations from site and/or metric code batches.

    This function takes batches and returns task configurations without
    mutating its inputs. It emits log messages for observability.

    Args:
        site_batches: Optional list of site ID batches
        metric_batches: Optional list of metric code batches

    Returns:
        List of tuples: (task_index, site_ids, metric_codes)
        - task_index: Sequential task number
        - site_ids: List of site IDs for this task (empty if no site batching)
        - metric_codes: List of metric codes for this task (empty if no metric batching)

    Behavior:
        - If both site_batches and metric_batches: Creates matrix (site × metric)
        - If only site_batches: One task per site batch
        - If only metric_batches: One task per metric batch
        - If neither: Single task with empty lists

    Examples:
        >>> generate_batch_tasks(site_batches=[[1, 2], [3, 4]])
        [(0, [1, 2], []),
         (1, [3, 4], [])]

        >>> generate_batch_tasks(
        ...     site_batches=[[1, 2]],
        ...     metric_batches=[['VOLTAGE', 'CURRENT']]
        ... )
        [(0, [1, 2], ['VOLTAGE', 'CURRENT'])]
    """
    if site_batches and metric_batches:
        # Matrix: site_batch × metric_batch
        data = []
        task_index = 0
        for site_batch in site_batches:
            for metric_batch in metric_batches:
                data.append((task_index, site_batch, metric_batch))
                task_index += 1
        logger.info(
            f"Creating matrix of {len(site_batches)} site batches × {len(metric_batches)} metric batches = {len(data)} total tasks"
        )
        return data

    elif site_batches:
        # Only site batching
        logger.info(f"Created {len(site_batches)} site-batched tasks")
        return [(e, site_batch, []) for e, site_batch in enumerate(site_batches)]

    elif metric_batches:
        # Only metric code batching
        logger.info(f"Created {len(metric_batches)} metric-code-batched tasks")
        return [(e, [], metric_batch) for e, metric_batch in enumerate(metric_batches)]

    else:
        # No batching - single task with no batch IDs
        logger.info("No batching configured - creating single task")
        return [(0, [], [])]
