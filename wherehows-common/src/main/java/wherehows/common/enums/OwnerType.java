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
package wherehows.common.enums;


public enum OwnerType {
  // the precedence (from high to low) is: OWNER, PRODUCER, DELEGATE, STAKEHOLDER
  OWNER(20),
  PRODUCER(40),
  DELEGATE(60),
  STAKEHOLDER(80);

  private int numVal;

  OwnerType(int numVal) {
    this.numVal = numVal;
  }

  public int value() {
    return numVal;
  }

  /**
   * return the owner type with higher priority (lower value)
   * @param type1
   * @param type2
   * @return
   */
  public static String chooseOwnerType(String type1, String type2) {
    int type1value = 100;
    try {
      type1value = OwnerType.valueOf(type1.toUpperCase()).value();
    } catch (NullPointerException | IllegalArgumentException ex) {
    }

    int type2value = 100;
    try {
      type2value = OwnerType.valueOf(type2.toUpperCase()).value();
    } catch (NullPointerException | IllegalArgumentException ex) {
    }

    return type1value <= type2value ? type1 : type2;
  }
}
