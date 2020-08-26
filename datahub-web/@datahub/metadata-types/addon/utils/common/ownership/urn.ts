import { OwnerUrnObject } from '@datahub/metadata-types/local-types/common/owner';

/**
 * Defines the expected regular expression pattern to match and expected owner urn string
 * examples urn:li:corpuser:ldap, urn:li:corpGroup:group_name, and urn:li:multiProduct:mp_name
 * @type {string}
 */
export const ownerUrnPattern = `urn:li:([\\w_-]+):([\\w_-]+)`;

/**
 * Constructs a RegExp instance using the ownerUrnPattern above for matching against potential urn strings
 * @type {RegExp}
 */
export const ownerUrnRegExp = new RegExp(ownerUrnPattern);

/**
 * Extracts the interesting parts that make up an owner urn string
 * @param {string} [ownerUrn=''] the urn for the urn to extracts into parts
 * @returns {OwnerUrnObject}
 */
export const getOwnerUrnParts = (ownerUrn = ''): OwnerUrnObject => {
  const urnParts: OwnerUrnObject = {
    type: undefined,
    name: undefined,
    _inputUrn: ownerUrn
  };

  const matchOrNull = ownerUrnRegExp.exec(ownerUrn);

  if (matchOrNull) {
    // Disregard the full string match
    const [, type, name] = matchOrNull;

    return {
      ...urnParts,
      type,
      name
    };
  }

  return urnParts;
};
