/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package wherehows.common.kafka.schemaregistry.avro;

public enum AvroCompatibilityLevel {
  NONE("NONE", AvroCompatibilityChecker.NO_OP_CHECKER),
  BACKWARD("BACKWARD", AvroCompatibilityChecker.BACKWARD_CHECKER),
  FORWARD("FORWARD", AvroCompatibilityChecker.FORWARD_CHECKER),
  FULL("FULL", AvroCompatibilityChecker.FULL_CHECKER);

  public final String name;
  public final AvroCompatibilityChecker compatibilityChecker;

  private AvroCompatibilityLevel(String name, AvroCompatibilityChecker compatibilityChecker) {
    this.name = name;
    this.compatibilityChecker = compatibilityChecker;
  }

  public static AvroCompatibilityLevel forName(String name) {
    if (name == null) {
      return null;
    }

    name = name.toUpperCase();
    if (NONE.name.equals(name)) {
      return NONE;
    } else if (BACKWARD.name.equals(name)) {
      return BACKWARD;
    } else if (FORWARD.name.equals(name)) {
      return FORWARD;
    } else if (FULL.name.equals(name)) {
      return FULL;
    } else {
      return null;
    }
  }
}
