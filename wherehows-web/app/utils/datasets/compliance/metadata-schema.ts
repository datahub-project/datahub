import { typeOf } from '@ember/utils';
import { DatasetClassifiers } from 'wherehows-web/constants';
import { arrayEvery, arrayMap, arrayReduce } from 'wherehows-web/utils/array';
import { IObject } from 'wherehows-web/typings/generic';
import { isObject } from 'wherehows-web/utils/object';

/**
 * Defines the interface for an IDL that specifies the data types for properties on
 * a compliance metadata object
 * @interface IMetadataType
 */
interface IMetadataType {
  // the expected type or types for the property with @name
  '@type': string | Array<string>;
  // the name of the property that should be on the metadata object
  '@name': string;
  // optional list of properties that are expected on the metadata object
  '@props'?: Array<IMetadataType>;
  // optional list of expected string values for an enum type (not implemented)
  '@symbols'?: Array<string>;
}

/**
 * Maps a datasetClassification property to the expected type of boolean
 * @param {string} prop
 * @returns {IMetadataType}
 */
const datasetClassificationPropType = (prop: string): IMetadataType => ({
  '@type': 'boolean',
  '@name': prop
});

/**
 * Lists the types for objects or instances in the the compliance metadata entities list
 * @type Array<IMetadataType>
 */
const complianceEntitiesTaxonomy: Array<IMetadataType> = [
  {
    '@type': 'array',
    '@name': 'complianceEntities',
    '@props': [
      {
        '@name': 'identifierField',
        '@type': 'string'
      },
      {
        '@name': 'identifierType',
        '@type': ['string', 'null']
      },
      {
        '@type': ['string', 'null'],
        '@name': 'logicalType'
      },
      {
        '@name': 'nonOwner',
        '@type': ['boolean', 'null']
      },
      {
        '@name': 'valuePattern',
        '@type': ['string', 'null']
      }
    ]
  }
];

/**
 * Defines the shape of the dataset compliance metadata json object using the IMetadataType interface
 * @type {Array<IMetadataType>}
 */
const complianceMetadataTaxonomy: Array<IMetadataType> = [
  {
    '@type': 'object',
    '@name': 'datasetClassification',
    '@props': arrayMap(datasetClassificationPropType)(Object.keys(DatasetClassifiers))
  },
  ...complianceEntitiesTaxonomy,
  {
    '@type': ['string', 'null'],
    '@name': 'compliancePurgeNote'
  },
  {
    '@type': 'string',
    '@name': 'complianceType'
  },
  {
    '@type': ['string', 'null'],
    '@name': 'confidentiality'
  }
];

/**
 * Checks that a value type matches an expected pattern string
 * @param {*} value the value to check
 * @param {string | Array<string>} expectedType the pattern string to match against
 * @returns {boolean}
 */
const valueEquiv = (value: any, expectedType: string | Array<string>): boolean => expectedType.includes(typeOf(value));

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
 * Returns a iteratee bound to an object that checks that a key matches the expected value in the typeMap
 * @param {IObject<any>} object the object with keys to check
 * @return {(metadataType: IMetadataType) => boolean}
 */
const keyValueHasMatch = (object: IObject<any>) => (metadataType: IMetadataType): boolean => {
  const [name, type] = typePatternMap(metadataType);
  const value = object[name];
  const rootValueEquiv = object.hasOwnProperty(name) && valueEquiv(value, type);
  const innerType = metadataType['@props'];

  if (type.includes('object') && isObject(value)) {
    // recurse on object properties
    return rootValueEquiv && keysEquiv(value, innerType!);
  }

  if (type.includes('array') && Array.isArray(value)) {
    const { length } = value;

    return (
      // recursively reduce on array elements
      // ensure the array contains at least on element
      rootValueEquiv &&
      length > 0 &&
      arrayReduce((isEquiv: boolean, value: any) => isEquiv && keysEquiv(value, innerType!), rootValueEquiv)(value)
    );
  }

  return rootValueEquiv;
};

/**
 * Ensures that the keys on the supplied object are equivalent to the names in the type definition list
 * @param {IObject<any>} object
 * @param {Array<IMetadataType>} typeMaps
 * @return {boolean}
 */
const keysMatchNames = (object: IObject<any>, typeMaps: Array<IMetadataType>): boolean =>
  Object.keys(object)
    .sort()
    .toString() ===
  arrayMap((typeMap: IMetadataType) => typeMap['@name'])(typeMaps)
    .sort()
    .toString();

/**
 * Checks each key on an object matches the expected types in the typeMap
 * @param {IObject<any>} object the object with keys to check
 * @param {Array<IMetadataType>} typeMaps the colon delimited type string
 * @returns {boolean}
 */
const keysEquiv = (object: IObject<any>, typeMaps: Array<IMetadataType>): boolean =>
  arrayEvery(keyValueHasMatch(object))(typeMaps) && keysMatchNames(object, typeMaps);

export default keysEquiv;

export { complianceMetadataTaxonomy, complianceEntitiesTaxonomy };
