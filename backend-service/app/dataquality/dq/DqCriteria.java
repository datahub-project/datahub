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

import com.fasterxml.jackson.databind.annotation.JsonSerialize;


/**
 * Created by zechen on 8/9/15.
 */
@JsonSerialize(include= JsonSerialize.Inclusion.NON_NULL)
public class DqCriteria {

  Double lt;

  Double gt;

  Double le;

  Double ge;

  Double eq;

  Double neq;

  private String detail = null;

  public DqCriteria() {
  }

  public DqCriteria(Double lt, Double gt, Double le, Double ge, Double eq, Double neq) {
    this.lt = lt;
    this.gt = gt;
    this.le = le;
    this.ge = ge;
    this.eq = eq;
    this.neq = neq;
  }

  public boolean check(Double value) {
    boolean ret = true;
    setDetail(null);
    StringBuilder detailStr = new StringBuilder("");
    if (lt != null && value.compareTo(lt) >= 0) {
      ret = false;
      detailStr.append("value is too big;");
    }

    if (le != null && value.compareTo(le) > 0) {
      ret = false;
      detailStr.append("value is too big;");
    }

    if (gt != null && value.compareTo(gt) <= 0) {
      ret = false;
      detailStr.append("value is too small;");
    }

    if (ge != null && value.compareTo(ge) < 0) {
      ret = false;
      detailStr.append("value is too small;");
    }

    if (eq != null && value.compareTo(eq) != 0) {
      ret = false;
      detailStr.append("value should equal to " + eq + ";");
    }

    if (neq != null && value.compareTo(neq) == 0) {
      ret = false;
      detailStr.append("value should not equal to " + neq + ";");
    }

    if (!ret && detailStr.length() != 0) {
      detail = detailStr.toString();
    }

    return ret;
  }

  public Double getLt() {
    return lt;
  }

  public void setLt(Double lt) {
    this.lt = lt;
  }

  public Double getGt() {
    return gt;
  }

  public void setGt(Double gt) {
    this.gt = gt;
  }

  public Double getLe() {
    return le;
  }

  public void setLe(Double le) {
    this.le = le;
  }

  public Double getGe() {
    return ge;
  }

  public void setGe(Double ge) {
    this.ge = ge;
  }

  public Double getEq() {
    return eq;
  }

  public void setEq(Double eq) {
    this.eq = eq;
  }

  public Double getNeq() {
    return neq;
  }

  public void setNeq(Double neq) {
    this.neq = neq;
  }

  public String getDetail() {
    return detail;
  }

  public void setDetail(String detail) {
    this.detail = detail;
  }
}
