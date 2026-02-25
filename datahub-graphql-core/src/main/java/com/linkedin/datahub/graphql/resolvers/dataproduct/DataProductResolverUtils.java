package com.linkedin.datahub.graphql.resolvers.dataproduct;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.service.DataProductService;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/** Utility methods shared across Data Product GraphQL resolvers. */
public class DataProductResolverUtils {

  private DataProductResolverUtils() {
    // Utility class, prevent instantiation
  }

  /**
   * Verifies that all resources exist and that the user has permission to update data products for
   * them.
   *
   * @param resources list of resource URN strings to verify
   * @param context the GraphQL query context
   * @param dataProductService service to check entity existence
   * @throws RuntimeException if any resource does not exist
   * @throws AuthorizationException if user is not authorized to update data products for any
   *     resource
   */
  public static void verifyResources(
      @Nonnull final List<String> resources,
      @Nonnull final QueryContext context,
      @Nonnull final DataProductService dataProductService) {
    for (String resource : resources) {
      final Urn resourceUrn = UrnUtils.getUrn(resource);
      if (!dataProductService.verifyEntityExists(context.getOperationContext(), resourceUrn)) {
        throw new RuntimeException(
            String.format("Failed to update data products, resource %s does not exist", resource));
      }
      if (!DataProductAuthorizationUtils.isAuthorizedToUpdateDataProductsForEntity(
          context, resourceUrn)) {
        throw new AuthorizationException(
            "Unauthorized to perform this action. Please contact your DataHub administrator.");
      }
    }
  }

  /**
   * Verifies that all data products exist.
   *
   * @param dataProductUrns list of data product URN strings to verify
   * @param context the GraphQL query context
   * @param dataProductService service to check entity existence
   * @throws RuntimeException if any data product does not exist
   */
  public static void verifyDataProducts(
      @Nonnull final List<String> dataProductUrns,
      @Nonnull final QueryContext context,
      @Nonnull final DataProductService dataProductService) {
    for (String dataProductUrn : dataProductUrns) {
      if (!dataProductService.verifyEntityExists(
          context.getOperationContext(), UrnUtils.getUrn(dataProductUrn))) {
        throw new RuntimeException(
            String.format(
                "Failed to update data products, data product %s does not exist", dataProductUrn));
      }
    }
  }

  /**
   * Verifies that a single data product exists (if not null).
   *
   * @param maybeDataProductUrn optional data product URN string to verify
   * @param context the GraphQL query context
   * @param dataProductService service to check entity existence
   * @throws RuntimeException if the data product does not exist
   */
  public static void verifyDataProduct(
      final String maybeDataProductUrn,
      @Nonnull final QueryContext context,
      @Nonnull final DataProductService dataProductService) {
    if (maybeDataProductUrn != null
        && !dataProductService.verifyEntityExists(
            context.getOperationContext(), UrnUtils.getUrn(maybeDataProductUrn))) {
      throw new RuntimeException(
          String.format(
              "Failed to update data products, data product %s does not exist",
              maybeDataProductUrn));
    }
  }

  /**
   * Converts a list of URN strings to ResourceReference objects.
   *
   * @param urnStrings list of URN strings
   * @return list of ResourceReference objects
   */
  @Nonnull
  public static List<ResourceReference> convertToResourceReferences(
      @Nonnull final List<String> urnStrings) {
    return urnStrings.stream()
        .map(UrnUtils::getUrn)
        .map(urn -> new ResourceReference(urn, null, null))
        .collect(Collectors.toList());
  }

  /**
   * Converts a list of Urn objects to ResourceReference objects.
   *
   * @param urns list of Urn objects
   * @return list of ResourceReference objects
   */
  @Nonnull
  public static List<ResourceReference> convertUrnsToResourceReferences(
      @Nonnull final List<Urn> urns) {
    return urns.stream()
        .map(urn -> new ResourceReference(urn, null, null))
        .collect(Collectors.toList());
  }
}
