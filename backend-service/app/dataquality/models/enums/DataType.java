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
package dataquality.models.enums;

/**
 * Enum for data type of user input and its corresponding mysql data type
 *
 * Created by zechen on 6/5/15.
 */


public enum DataType {

  INT("INT"),
  INTEGER("INT"),
  LONG("BIGINT"),
  DOUBLE("DOUBLE"),
  STRING("VARCHAR(255)"),
  DATE("DATE"),
  DATETIME("DATETIME"),
  TIMESTAMP("TIMESTAMP"),
  ;

  private String myType;

  DataType(String myType) {
    this.myType = myType;
  }

  public String getMyType() {
    return myType;
  }
}
