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
package dataquality.utils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import org.kie.api.runtime.rule.AccumulateFunction;


/**
 * Created by zechen on 8/5/15.
 */
@Deprecated
public class DiffFunction implements AccumulateFunction {

  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

  }

  public void writeExternal(ObjectOutput out) throws IOException {

  }

  public static class AverageData implements Externalizable {
    public int    count = 0;
    public double total = 0;

    public AverageData() {}

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      count   = in.readInt();
      total   = in.readDouble();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeInt(count);
      out.writeDouble(total);
    }

  }

  /* (non-Javadoc)
   * @see org.kie.base.accumulators.AccumulateFunction#createContext()
   */
  public Serializable createContext() {
    return new AverageData();
  }

  /* (non-Javadoc)
   * @see org.kie.base.accumulators.AccumulateFunction#init(java.lang.Object)
   */
  public void init(Serializable context) throws Exception {
    AverageData data = (AverageData) context;
    data.count = 0;
    data.total = 0;
  }

  /* (non-Javadoc)
   * @see org.kie.base.accumulators.AccumulateFunction#accumulate(java.lang.Object, java.lang.Object)
   */
  public void accumulate(Serializable context,
      Object value) {
    AverageData data = (AverageData) context;
    data.count++;
    data.total += ((Number) value).doubleValue();
  }

  /* (non-Javadoc)
   * @see org.kie.base.accumulators.AccumulateFunction#reverse(java.lang.Object, java.lang.Object)
   */
  public void reverse(Serializable context,
      Object value) throws Exception {
    AverageData data = (AverageData) context;
    data.count--;
    data.total -= ((Number) value).doubleValue();
  }

  /* (non-Javadoc)
   * @see org.kie.base.accumulators.AccumulateFunction#getResult(java.lang.Object)
   */
  public Object getResult(Serializable context) throws Exception {
    AverageData data = (AverageData) context;
    return new Double( data.count == 0 ? 0 : data.total / data.count );
  }

  /* (non-Javadoc)
   * @see org.kie.base.accumulators.AccumulateFunction#supportsReverse()
   */
  public boolean supportsReverse() {
    return true;
  }

  /**
   * {@inheritDoc}
   */
  public Class< ? > getResultType() {
    return Number.class;
  }

}

