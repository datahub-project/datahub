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
package dataquality.dq;

import java.util.Map;


/**
 * Created by zechen on 8/3/15.
 */
public class DqRule {
  DqFunctionType func;

  Map<String, Object> params;

  DqCriteria criteria;

  public DqFunctionType getFunc() {
    return func;
  }

  public void setFunc(DqFunctionType func) {
    this.func = func;
  }

  public Map<String, Object> getParams() {
    return params;
  }

  public void setParams(Map<String, Object> params) {
    this.params = params;
  }

  public DqCriteria getCriteria() {
    return criteria;
  }

  public void setCriteria(DqCriteria criteria) {
    this.criteria = criteria;
  }
}
