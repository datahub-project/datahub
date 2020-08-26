import { getOwnerUrnParts } from '@datahub/metadata-types/utils/common/ownership/urn';

/**
 * Iteratee extracts the name property from an owner urn string, undefined if not found
 * @param {IOwner} { owner } an instance of an IOwner
 * @returns {(string | undefined)}
 */
export const getOwnerUserName = ({ owner }: Com.Linkedin.Common.Owner): string | undefined =>
  getOwnerUrnParts(owner).name;
