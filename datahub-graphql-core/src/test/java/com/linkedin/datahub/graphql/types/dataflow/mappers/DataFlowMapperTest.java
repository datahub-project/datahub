package com.linkedin.datahub.graphql.types.dataflow.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.DataFlow;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DataFlowMapperTest {
  private static final Urn TEST_DATA_FLOW_URN =
      Urn.createFromTuple(Constants.DATA_FLOW_ENTITY_NAME, "dataflow1");
  private static final Urn TEST_CONTAINER_URN =
      Urn.createFromTuple(Constants.CONTAINER_ENTITY_NAME, "container1");

  @Test
  public void testMapDataFlowContainer() throws URISyntaxException {
    com.linkedin.container.Container input = new com.linkedin.container.Container();
    input.setContainer(TEST_CONTAINER_URN);

    final Map<String, EnvelopedAspect> containerAspect = new HashMap<>();
    containerAspect.put(
        Constants.CONTAINER_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect().setValue(new Aspect(input.data())));
    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DATA_FLOW_ENTITY_NAME)
            .setUrn(TEST_DATA_FLOW_URN)
            .setAspects(new EnvelopedAspectMap(containerAspect));

    final DataFlow actual = DataFlowMapper.map(null, response);

    Assert.assertEquals(actual.getUrn(), TEST_DATA_FLOW_URN.toString());
    Assert.assertEquals(actual.getContainer().getUrn(), TEST_CONTAINER_URN.toString());
  }
}
