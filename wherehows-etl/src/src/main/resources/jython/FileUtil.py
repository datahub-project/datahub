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

from distutils.util import strtobool
from wherehows.common import Constant

def etl_temp_dir(args, etl_type):
    dir = os.path.join(args[Constant.WH_APP_FOLDER_KEY], etl_type, args[Constant.WH_EXEC_ID_KEY])
    if not os.path.exists(dir):
        os.makedirs(dir)

    return dir


def parse_bool(value, default):
    """
    parse string to bool value, if error, return default.
    True values are y, yes, t, true, on and 1; false values are n, no, f, false, off and 0.
    :param value: input string value
    :param default: default output value (True/False) if parsing error
    :return: boolean
    """
    try:
        # Use 'strtobool'. True values are y, yes, t, true, on and 1; false values are n, no, f, false, off and 0.
        # Raises ValueError if val is anything else.
        return strtobool(value)
    except ValueError:
        return default
