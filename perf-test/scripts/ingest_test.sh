# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

locust -f ./locustfiles/ingest.py --autostart --autoquit 1 --only-summary -t 30s --host http://localhost:8080 -u 10 -r 1
locust -f ./locustfiles/ingest.py --autostart --autoquit 1 --only-summary -t 150s --host http://localhost:8080 -u 100 -r 2
locust -f ./locustfiles/ingest.py --autostart --autoquit 1 --only-summary -t 300s --host http://localhost:8080 -u 500 -r 5
locust -f ./locustfiles/ingest.py --autostart --autoquit 1 --only-summary -t 300s --host http://localhost:8080 -u 1000 -r 10