/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

db.createCollection("purchases");
db.purchases.insertOne({ _id: 3, item: "lamp post", price: 12 });
db.purchases.insertOne({ _id: 4, item: "lamp post", price: 13 });
