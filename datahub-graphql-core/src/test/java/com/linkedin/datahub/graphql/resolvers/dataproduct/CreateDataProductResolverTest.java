package com.linkedin.datahub.graphql.resolvers.dataproduct;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateDataProductInput;
import com.linkedin.datahub.graphql.generated.CreateDataProductPropertiesInput;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.DataProductService;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.testng.annotations.Test;

public class CreateDataProductResolverTest {

  @Test
  public void testRejectsNonDomainUrnBeforeWriting() {
    // The domain privilege check accepts EDIT_ENTITY on whatever URN it is given, and a non-domain
    // URN is only rejected by setDomain, after dataProductProperties has been committed.
    DataProductService dataProductService = mock(DataProductService.class);
    EntityService<?> entityService = mock(EntityService.class);
    CreateDataProductResolver resolver =
        new CreateDataProductResolver(dataProductService, entityService);

    CreateDataProductPropertiesInput properties = new CreateDataProductPropertiesInput();
    properties.setName("Product");
    CreateDataProductInput input = new CreateDataProductInput();
    input.setProperties(properties);
    input.setDomainUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)");

    QueryContext context = getMockAllowContext();
    DataFetchingEnvironment environment = mock(DataFetchingEnvironment.class);
    when(environment.getArgument(eq("input"))).thenReturn(input);
    when(environment.getContext()).thenReturn(context);

    assertThrows(CompletionException.class, () -> resolver.get(environment).join());
    verify(dataProductService, never()).createDataProduct(any(), any(), any(), any(), any());
    verify(dataProductService, never()).setDomain(any(), any(), any());
  }
}
