/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

rsconf = {
  _id: "rs0",
  members: [{ _id: 0, host: "test_mongo:27017", priority: 1.0 }],
};
rs.initiate(rsconf);
rs.status();
