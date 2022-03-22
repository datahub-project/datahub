from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class BaseTimeWindowReport:
    end_time: Optional[datetime] = None
    start_time: Optional[datetime] = None
