#
# Copyright 2015 LinkedIn Corp. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#

from .charsetgroupprober import CharSetGroupProber
from .utf8prober import UTF8Prober
from .sjisprober import SJISProber
from .eucjpprober import EUCJPProber
from .gb2312prober import GB2312Prober
from .euckrprober import EUCKRProber
from .cp949prober import CP949Prober
from .big5prober import Big5Prober
from .euctwprober import EUCTWProber


class MBCSGroupProber(CharSetGroupProber):
    def __init__(self):
        CharSetGroupProber.__init__(self)
        self._mProbers = [
            UTF8Prober(),
            SJISProber(),
            EUCJPProber(),
            GB2312Prober(),
            EUCKRProber(),
            CP949Prober(),
            Big5Prober(),
            EUCTWProber()
        ]
        self.reset()
