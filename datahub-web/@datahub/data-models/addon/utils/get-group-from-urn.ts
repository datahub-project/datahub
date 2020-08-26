import { gridGroupUrnBasePrefix } from '@datahub/data-models/config/urn/base-prefix';

/**
 * Returns the grid group name from its urn.
 * @param urn - expected to be a urn for a grid group
 */
export const getGridGroupFromUrn = (urn: string): string => urn.replace(gridGroupUrnBasePrefix, '');
