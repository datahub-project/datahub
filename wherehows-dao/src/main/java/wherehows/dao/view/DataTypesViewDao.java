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
package wherehows.dao.view;

import java.util.List;
import java.util.Map;
import javax.persistence.EntityManagerFactory;


public class DataTypesViewDao extends BaseViewDao {

  public DataTypesViewDao(EntityManagerFactory factory) {
    super(factory);
  }

  /**
   * Get all compliance dataTypes
   */
  public List<Map<String, Object>> getAllComplianceDataTypes() throws Exception {
    throw new UnsupportedOperationException("Operation not implemented");
  }

  /**
   * Get all data platforms
   */
  public List<Map<String, Object>> getAllPlatforms() throws Exception {
    throw new UnsupportedOperationException("Operation not implemented");
  }
}
