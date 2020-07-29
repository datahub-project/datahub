package com.linkedin.metadata.restli;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.metadata.query.BrowseResultEntityArray;
import com.linkedin.metadata.query.BrowseResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.common.CollectionMetadata;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.testing.EntityValue;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

public class BaseBrowsableClientTest {

  private Client _mockRestClient;

  public static class TestBrowsableClient extends BaseBrowsableClient<EntityValue, Urn> {

    public TestBrowsableClient(@Nonnull Client restliClient) {
      super(restliClient);
    }

    @Override
    @Nonnull
    public BrowseResult browse(@Nonnull String inputPath, @Nullable Map<String, String> requestFilters, int from, int size) throws RemoteInvocationException {
      BrowseResultMetadata browseResultMetadata = new BrowseResultMetadata().setTotalNumEntities(100);
      return new BrowseResult().setEntities(new BrowseResultEntityArray()).setFrom(0).setPageSize(10).setMetadata(browseResultMetadata).setNumEntities(8);
    }

    @Override
    @Nonnull
    public StringArray getBrowsePaths(@Nonnull Urn urn) throws RemoteInvocationException {
      return new StringArray(Arrays.asList("/root/path1", "/root/path2", "/root/path3"));
    }

    @Override
    @Nonnull
    public CollectionResponse<EntityValue> search(@Nonnull String input, @Nonnull StringArray aspectNames, @Nullable Map<String, String> requestFilters,
        @Nullable SortCriterion sortCriterion, int start, int count) throws RemoteInvocationException {
      CollectionResponse<EntityValue> collectionResponse = new CollectionResponse<>(EntityValue.class);
      collectionResponse.setPaging(new CollectionMetadata().setTotal(200));
      return collectionResponse;
    }
  }

  @BeforeMethod
  public void setup() {
    _mockRestClient = mock(Client.class);
  }

  @Test
  public void testClient() throws RemoteInvocationException {
    TestBrowsableClient testBrowsableClient = new TestBrowsableClient(_mockRestClient);
    assertEquals(testBrowsableClient.search("test", new StringArray(), new HashMap<>(), null, 0,
        10).getPaging().getTotal().intValue(), 200);
    assertEquals(testBrowsableClient.browse("/root", null, 0, 10).getNumEntities().intValue(), 8);
    assertEquals(testBrowsableClient.browse("/root", null, 0, 10).getPageSize().intValue(), 10);
  }

}