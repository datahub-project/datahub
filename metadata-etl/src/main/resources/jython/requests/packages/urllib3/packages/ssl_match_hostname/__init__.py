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

try:
    # Python 3.2+
    from ssl import CertificateError, match_hostname
except ImportError:
    try:
        # Backport of the function from a pypi module
        from backports.ssl_match_hostname import CertificateError, match_hostname
    except ImportError:
        # Our vendored copy
        from ._implementation import CertificateError, match_hostname

# Not needed, but documenting what we provide.
__all__ = ('CertificateError', 'match_hostname')
