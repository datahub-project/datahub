import { typeOf } from '@ember/utils';
import { DatasetClassifiers } from 'wherehows-web/constants';
import { arrayEvery, arrayReduce } from 'wherehows-web/utils/array';
import { Classification } from 'wherehows-web/constants/datasets/compliance';
import { IObject } from 'wherehows-web/typings/generic';

/**
 * Describes the interface for schemas that are used for compliance metadata objects
 * @interface IMetadataTaxonomy
 */
interface IMetadataTaxonomy {
  readonly '@props': Array<string>;
  readonly '@type': string;

  readonly [K: string]: IMetadataTaxonomy | any;
}

/**
 * Defines the shape of the dataset compliance metadata json object using the IMetadataTaxonomy interface
 * @type {({'@type': string; '@props': string[]} | {'@type': string; '@props': string[]; securityClassification: {'@type': string; '@props': any[]}})[]}
 */
const datasetComplianceMetadataTaxonomy: Array<IMetadataTaxonomy> = [
  {
    '@type': 'datasetClassification:object',
    '@props': Object.keys(DatasetClassifiers).map(key => `${key}:boolean`)
  },
  {
    '@type': 'complianceEntities:array',
    '@props': [
      'identifierField:string',
      'identifierType:string|null',
      'securityClassification:string|null',
      'logicalType:string|null',
      'nonOwner:boolean|null',
      'valuePattern:string|null'
    ],
    securityClassification: {
      '@type': 'securityClassification:string',
      '@props': Object.values(Classification)
    }
  }
];

/**
 * Checks that a value type matches an expected pattern string
 * @param {*} value the value to check
 * @param {string} expectedTypePattern the pattern string to match against
 * @returns {boolean}
 */
const valueEquiv = (value: any, expectedTypePattern: string): boolean => expectedTypePattern.includes(typeOf(value));

/**
 * Extracts the type key and the pattern string from the string mapping into a tuple pair
 * @param {string} objectKeyTypePattern string value consisting of a pair of key/property name and allowed types separated by a colon ":"
 * @returns {[string, string]}
 */
const typePatternMap = (objectKeyTypePattern: string): [string, string] =>
  <[string, string]>objectKeyTypePattern.split(':');

/**
 * Returns a iteratee bound to an object that checks that a key matches the expected value in the typeMap
 * @param {IObject<any>} object the object with keys to check
 * @return {(typeMap: string) => boolean}
 */
const keyValueHasMatch = (object: IObject<any>) => (typeMap: string) => {
  const [key, typeString] = typePatternMap(typeMap);
  return valueEquiv(object[key], typeString);
};

/**
 * Checks each key on an object matches the expected types in the typeMap
 * @param {IObject<any>} object the object with keys to check
 * @param {Array<string>} typeMaps the colon delimited type string
 * @returns {boolean}
 */
const keysEquiv = (object: IObject<any>, typeMaps: Array<string>): boolean =>
  arrayEvery(keyValueHasMatch(object))(typeMaps);

/**
 * Checks that a compliance metadata object has a schema that matches the taxonomy / schema provided
 * @param {IObject<any>} object an instance of a compliance metadata object
 * @param {Array<IMetadataTaxonomy>} taxonomy schema shape to check against
 * @return {boolean}
 */
const validateMetadataObject = (object: IObject<any>, taxonomy: Array<IMetadataTaxonomy>): boolean => {
  const rootTypeMaps = taxonomy.map(category => category['@type']);
  let isValid = keysEquiv(object, rootTypeMaps);

  if (isValid) {
    const downlevelAccumulator = (validity: boolean, typeMap: string): boolean => {
      const [key, pattern]: [string, string] = typePatternMap(typeMap);

      if (pattern.includes('object')) {
        validity = keysEquiv(object[key], taxonomy.findBy('@type', typeMap)!['@props']);
      }

      if (pattern.includes('array') && Array.isArray(object[key])) {
        validity = arrayReduce(
          (validity: boolean, value: IObject<string>) =>
            validity && keysEquiv(value, taxonomy.findBy('@type', typeMap)!['@props']),
          validity
        )(object[key]);
      }

      return validity;
    };

    return arrayReduce(downlevelAccumulator, isValid)(rootTypeMaps);
  }

  return isValid;
};

export default validateMetadataObject;

export { datasetComplianceMetadataTaxonomy };
