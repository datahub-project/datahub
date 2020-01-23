import { IOwner } from '@datahub/metadata-types/types/common/owner';
import { getOwnerUrnParts } from '@datahub/metadata-types/utils/common/ownership/urn';

/**
 * Iteratee extracts the name property from an owner urn string, undefined if not found
 * @param {IOwner} { owner } an instance of an IOwner
 * @returns {(string | undefined)}
 */
export const getOwnerUserName = ({ owner }: IOwner): string | undefined => getOwnerUrnParts(owner).name;
