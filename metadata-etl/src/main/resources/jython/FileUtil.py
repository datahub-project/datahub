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

import os

from wherehows.common import Constant

def etl_temp_dir(args, etl_type):
    dir = os.path.join(args[Constant.WH_APP_FOLDER_KEY], etl_type, args[Constant.WH_EXEC_ID_KEY])
    if not os.path.exists(dir):
        os.makedirs(dir)

    return dir


