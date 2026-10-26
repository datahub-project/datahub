import useBulkDataProductMemberships from '@app/lineageV3/queries/useBulkDataProductMemberships';

/**
 * Fan-out for bulk bounding-box membership resolvers. Each inner hook self-skips
 * unless it matches the current rootType (see `useBulkDataProductMemberships`).
 * Add a sibling hook when a new rootType gains a bulk membership API, and add
 * its EntityType to BOUNDING_BOX_MEMBERSHIP_RESOLVED_ROOT_TYPES.
 */
export default function useBulkBoundingBoxMemberships() {
    useBulkDataProductMemberships();
    // Future: useBulkSemanticModelMemberships();
}
