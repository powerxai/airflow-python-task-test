import logging
import math
from typing import TypeVar

logger = logging.getLogger("airflow.task")
T = TypeVar("T")

def even_batches(data: list[T], size: int) -> list[list[T]]:
    """
    Split data into batches of approximately equal size.

    Args:
        data: List of items to batch
        size: Target batch size

    Returns:
        List of batches

    Raises:
        ValueError: If size is <= 0
    """
    try:
        if size <= 0:
            raise ValueError(f"Batch size must be positive, got {size}")

        if not data:
            logger.error("Empty data list provided to even_batches")
            return []

        logger.info(f"Creating batches from {len(data)} items with target size {size}")

        batches = math.ceil(len(data) / size)
        even_batch_size = math.ceil(len(data) / batches)

        result = [
            data[i : i + even_batch_size] for i in range(0, len(data), even_batch_size)
        ]

        logger.info(
            f"Created {len(result)} batches with sizes: {[len(batch) for batch in result]}"
        )

        return result

    except Exception as e:
        logger.error(f"Error in even_batches: {e}")
        raise