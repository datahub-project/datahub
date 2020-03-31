package com.linkedin.testing;

import com.linkedin.common.urn.Urn;
import com.linkedin.testing.urn.BarUrn;
import com.linkedin.testing.urn.BazUrn;
import com.linkedin.testing.urn.FooUrn;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;


public class TestUtils {

  private TestUtils() {
    // util class
  }

  @Nonnull
  public static FooUrn makeFooUrn(int id) {
    try {
      return new FooUrn(id);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  public static BarUrn makeBarUrn(int id) {
    try {
      return new BarUrn(id);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  public static BazUrn makeBazUrn(int id) {
    try {
      return new BazUrn(id);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  public static <T> Urn makeUrn(@Nonnull T id) {
    try {
      return new Urn("urn:li:testing:" + id);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  public static <T> Urn makeUrn(@Nonnull T id, @Nonnull String entityType) {
    try {
      return new Urn("urn:li:" + entityType + ":" + id);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  public static EntityKey makeKey(long id) {
    return new EntityKey().setId(id);
  }

  @Nonnull
  public static ComplexResourceKey<EntityKey, EmptyRecord> makeResourceKey(@Nonnull Urn urn) {
    return new ComplexResourceKey<>(makeKey(urn.getIdAsLong()), new EmptyRecord());
  }

  @Nonnull
  public static EntityDocument makeDocument(@Nonnull Urn urn) {
    return new EntityDocument().setUrn(urn);
  }
}
