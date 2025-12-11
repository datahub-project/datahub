# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import random
import string

"""
Generates a large "random" collection of fields to populate mongo init script
Run it with `python generate_large_collection.py >> mongo_init.js` to add it to the init script
"""

num_fields = 500
# first 200 fields have low probability
fields = [(f"field_{i}", (10 if i < 200 else 99)) for i in range(0, 500)]


letters = string.ascii_lowercase


def generate_record(fields):
    record = {}
    for f_name, probability in fields:
        dice = random.randint(0, 100)
        if dice < probability:
            record[f_name] = "".join(random.choice(letters) for i in range(0, 10))
    return record


print("db.largeCollection.insertMany([")
for x in range(0, 100):
    x = generate_record(fields)
    print(f"{x},")

print("]);")
