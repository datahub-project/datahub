import { IMetadataType } from '@datahub/data-models/types/entity/validators';
import { every, difference } from 'lodash';
import { typeOf } from '@ember/utils';
import { isObject } from '@datahub/utils/validators/object';

/**
 * Extracts the type key and the pattern string from the string mapping into a tuple pair
 * @param {IMetadataType} metadataType
 * @return {[string , (string | Array<string>)]}
 */
const typePatternMap = (metadataType: IMetadataType): [string, string | Array<string>] => [
  metadataType['@name'],
  metadataType['@type']
];

/**
 * Checks that a value type matches an expected pattern string
 * @param {*} value the value to check
 * @param {string | Array<string>} expectedType the pattern string to match against
 * @returns {boolean}
 */
const isValueEquiv = (value: unknown, expectedType: string | Array<string>): true => {
  const valueType = typeOf(value);
  const isValueOfExpectedType = expectedType.includes(valueType);

  if (!isValueOfExpectedType) {
    throw new Error(`Expected ${value} to be of type(s) ${expectedType}, got ${valueType}`);
  }

  return isValueOfExpectedType;
};

/**
 * Returns a iteratee bound to an object that checks that a key matches the expected value in the typeMap
 * @param {Record<string,unknown>} object the object with keys to check
 * @return {(metadataType: IMetadataType) => boolean}
 */
// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
const keyValueHasMatch = (object: Record<string, unknown>) => (metadataType: IMetadataType): boolean => {
  const [name, type] = typePatternMap(metadataType);
  const value = object[name];
  const isRootValueEquiv = object.hasOwnProperty(name) && isValueEquiv(value, type);
  const innerType = metadataType['@props'];

  if (!isRootValueEquiv) {
    throw new Error(`Expected "${name}" to be a key on object with the identifierField: "${object.identifierField}`);
  }

  if (type.includes('object') && isObject(value)) {
    // recurse on object properties
    return isRootValueEquiv && keysEquiv(value, innerType as Array<IMetadataType>); // eslint-disable-line @typescript-eslint/no-use-before-define
  }

  if (type.includes('array') && Array.isArray(value)) {
    const { length } = value;

    if (!length) {
      throw new Error(`Expected array for ${name} to not be empty`);
    }

    return (
      // recursively reduce on array elements
      // ensure the array contains at least on element
      isRootValueEquiv &&
      length > 0 &&
      value.reduce(
        // eslint-disable-next-line @typescript-eslint/no-use-before-define
        (isEquiv: boolean, value) => isEquiv && keysEquiv(value, innerType as Array<IMetadataType>),
        isRootValueEquiv
      )
    );
  }

  return isRootValueEquiv;
};

/**
 * Ensures that the keys on the supplied object are equivalent to the names in the type definition list
 * @param {Record<string, unknown>} object
 * @param {Array<IMetadataType>} typeMaps
 * @return {boolean}
 * @throws {Error} if object keys do not match type @names
 */
const keysMatchNames = (object: Record<string, unknown>, typeMaps: Array<IMetadataType>): true => {
  const objectKeys = Object.keys(object).sort();
  const typeKeys = typeMaps.map((typeMap: IMetadataType) => typeMap['@name']).sort();
  const objectKeysSerialized = objectKeys.toString();
  const typeKeysSerialized = typeKeys.toString();
  const match = objectKeysSerialized === typeKeysSerialized;

  if (!match) {
    throw new Error(
      `Expected attributes ${typeKeys.join(', ')} on object. Found additional attributes ${difference(
        objectKeys,
        typeKeys
      ).join(', ')}`
    );
  }

  return match;
};

/**
 * Checks each key on an object matches the expected types in the typeMap
 * @param {object} object the object with keys to check
 * @param {Array<IMetadataType>} typeMaps the colon delimited type string
 * @returns {boolean}
 */
// This object value is based on parsed user input and can truly take on any value
export const keysEquiv = (object: {}, typeMaps: Array<IMetadataType>): boolean =>
  every(typeMaps, keyValueHasMatch(object)) && keysMatchNames(object, typeMaps);
