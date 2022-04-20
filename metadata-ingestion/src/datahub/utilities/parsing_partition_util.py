import datetime
import logging
from typing import Tuple

from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)


def get_partition_range_from_partition_id(
    partition_id: str, partition_datetime: datetime.datetime
) -> Tuple[datetime.datetime, datetime.datetime]:
    duration: relativedelta
    # if yearly partitioned,
    if len(partition_id) == 4:
        duration = relativedelta(years=1)
        partition_datetime = partition_datetime.replace(month=1, day=1)
    # elif monthly partitioned,
    elif len(partition_id) == 6:
        duration = relativedelta(months=1)
        partition_datetime = partition_datetime.replace(day=1)
    # elif daily partitioned,
    elif len(partition_id) == 8:
        duration = relativedelta(days=1)
    # elif hourly partitioned,
    elif len(partition_id) == 10:
        duration = relativedelta(hours=1)
    upper_bound_partition_datetime = partition_datetime + duration
    return partition_datetime, upper_bound_partition_datetime
