package com.linkedin.datahub.graphql.types.dataprocessinst.mappers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.DataProcessInstance;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import org.testng.annotations.Test;

public class DataProcessInstanceMapperTest {

    @Test
    public void testMap() throws Exception {
        EntityResponse entityResponse = new EntityResponse();
        Urn urn = Urn.createFromString("urn:li:dataProcessInstance:(test-workflow,test-instance)");
        entityResponse.setUrn(urn);
        entityResponse.setAspects(new EnvelopedAspectMap(ImmutableMap.of()));

        DataProcessInstance instance = DataProcessInstanceMapper.map(null, entityResponse);

        assertNotNull(instance);
        assertEquals(instance.getUrn(), urn.toString());
        assertEquals(instance.getType(), EntityType.DATA_PROCESS_INSTANCE);
    }
}