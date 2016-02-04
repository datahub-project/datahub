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

from jython.SchedulerLoad import SchedulerLoad
import sys


class OozieLoad(SchedulerLoad):
  def __init__(self, args):
    SchedulerLoad.__init__(self, args)


if __name__ == "__main__":
  props = sys.argv[1]
  oz = OozieLoad(props)
  oz.run()
