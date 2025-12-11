# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class BaseTimeWindowReport:
    window_end_time: Optional[datetime] = None
    window_start_time: Optional[datetime] = None
