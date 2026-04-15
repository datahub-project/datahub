package com.linkedin.datahub.graphql.resolvers.dataproduct;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.metadata.service.DataProductService;
import java.util.List;
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
      verifyDataProduct(dataProductUrn, context, dataProductService);
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
}
