package com.linkedin.metadata.restli;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.dao.BaseBrowseDAO;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.metadata.query.BrowseResultEntity;
import com.linkedin.metadata.query.BrowseResultEntityArray;
import com.linkedin.metadata.query.BrowseResultMetadata;
import com.linkedin.parseq.BaseEngineTest;
import com.linkedin.testing.EntityAspectUnion;
import com.linkedin.testing.EntityDocument;
import com.linkedin.testing.EntityKey;
import com.linkedin.testing.EntitySnapshot;
import com.linkedin.testing.EntityValue;
import java.net.URISyntaxException;
import java.util.List;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.utils.QueryUtils.*;
import static com.linkedin.testing.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BaseBrowsableEntityResourceTest extends BaseEngineTest {

  private BaseBrowseDAO _mockBrowseDAO;
  private TestResource _resource = new TestResource();

  class TestResource extends BaseBrowsableEntityResource<
      // format
      EntityKey, EntityValue, Urn, EntitySnapshot, EntityAspectUnion, EntityDocument> {

    public TestResource() {
      super(EntitySnapshot.class, EntityAspectUnion.class);
    }

    @Nonnull
    @Override
    protected BaseLocalDAO<EntityAspectUnion, Urn> getLocalDAO() {
      throw new RuntimeException("Not implemented");
    }

    @Nonnull
    @Override
    protected BaseSearchDAO getSearchDAO() {
      throw new RuntimeException("Not implemented");
    }

    @Nonnull
    @Override
    protected BaseBrowseDAO getBrowseDAO() {
      return _mockBrowseDAO;
    }

    @Nonnull
    @Override
    protected Urn createUrnFromString(@Nonnull String urnString) {
      try {
        return Urn.createFromString(urnString);
      } catch (URISyntaxException e) {
        throw RestliUtils.badRequestException("Invalid URN: " + urnString);
      }
    }

    @Nonnull
    @Override
    protected Urn toUrn(@Nonnull EntityKey key) {
      throw new RuntimeException("Not implemented");
    }

    @Nonnull
    @Override
    protected EntityKey toKey(@Nonnull Urn urn) {
      throw new RuntimeException("Not implemented");
    }

    @Nonnull
    @Override
    protected EntityValue toValue(@Nonnull EntitySnapshot snapshot) {
      throw new RuntimeException("Not implemented");
    }

    @Nonnull
    @Override
    protected EntitySnapshot toSnapshot(@Nonnull EntityValue value, @Nonnull Urn urn) {
      throw new RuntimeException("Not implemented");
    }
  }

  @BeforeMethod
  public void setup() {
    _mockBrowseDAO = mock(BaseBrowseDAO.class);
  }

  @Test
  public void testBrowse() {
    BrowseResultEntityArray entities = new BrowseResultEntityArray(
        ImmutableList.of(makeBrowseResultEntity("/foo/1", makeUrn(1)), makeBrowseResultEntity("/foo/2", makeUrn(2))));
    BrowseResult expected = new BrowseResult().setEntities(entities)
        .setMetadata(new BrowseResultMetadata())
        .setFrom(1)
        .setPageSize(2)
        .setNumEntities(3);

    when(_mockBrowseDAO.browse("/foo", EMPTY_FILTER, 1, 2)).thenReturn(expected);

    BrowseResult result = runAndWait(_resource.browse("/foo", EMPTY_FILTER, 1, 2));

    assertEquals(result, expected);
  }

  @Test
  public void testGetBrowsePaths() {
    Urn urn = makeUrn(1);
    List<String> expected = ImmutableList.of("/foo", "/bar", "/baz");

    when(_mockBrowseDAO.getBrowsePaths(urn)).thenReturn(expected);

    StringArray paths = runAndWait(_resource.getBrowsePaths(urn));

    assertEquals(paths, new StringArray(expected));
  }

  private BrowseResultEntity makeBrowseResultEntity(String name, Urn urn) {
    return new BrowseResultEntity().setName(name).setUrn(urn);
  }
}
