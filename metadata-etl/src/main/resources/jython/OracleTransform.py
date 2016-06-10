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

# Changed by aig on 2016-06-10

import sys, os
from org.slf4j import LoggerFactory


class OracleTransform:
    def __init__(self):
        self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

if __name__ == "__main__":
    args = sys.argv[1]
    t = OracleTransform()
    t.log_file = args['oracle.log']

