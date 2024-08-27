from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class BaseTimeWindowReport:
    window_end_time: Optional[datetime] = None
    window_start_time: Optional[datetime] = None
