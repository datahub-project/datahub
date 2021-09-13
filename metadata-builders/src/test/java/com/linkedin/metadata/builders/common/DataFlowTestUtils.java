package com.linkedin.metadata.builders.common;

import com.linkedin.metadata.aspect.DataFlowAspect;
import com.linkedin.datajob.DataFlowInfo;

import static com.linkedin.metadata.testing.Owners.*;
import static com.linkedin.metadata.testing.Urns.*;

public class DataFlowTestUtils {

  private DataFlowTestUtils() {
    // Util class should not have public constructor
  }
  public static DataFlowAspect makeOwnershipAspect() {
    DataFlowAspect aspect = new DataFlowAspect();
    aspect.setOwnership(makeOwnership("fooUser"));
    return aspect;
  }

  public static DataFlowAspect makeDataFlowInfoAspect() {
    DataFlowInfo dataFlowInfo = new DataFlowInfo();
    dataFlowInfo.setName("Flow number 1");
    dataFlowInfo.setDescription("A description");
    dataFlowInfo.setProject("Lost cause");
    DataFlowAspect aspect = new DataFlowAspect();
    aspect.setDataFlowInfo(dataFlowInfo);
    return aspect;
  }
}
