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
package wherehows.common.schemas;

import com.fasterxml.jackson.annotation.JsonIgnore;


/**
 * The {@code Record} define the data interface of metadata ETL.
 * <p>
 * The Extract and Transform process are very flexible and will be highly customized,
 * but the Load process will be similar as we have the predefined data model (in mysql database).
 * A Record object will hold one record (correspond to record in database).
 * <p>
 * {@link wherehows.common.writers.DatabaseWriter} & {@link wherehows.common.writers.FileWriter} contain Record objects, then they can either directly insert this record in database or
 * write this record into csv file and batch load into mysql.
 */
public interface Record {
  /**
   * Convert to csv string that will write to csv file
   */
  @JsonIgnore
  public String toCsvString();

  /**
   * Convert to database value that will append into sql
   */
  @JsonIgnore
  public String toDatabaseValue();
}
