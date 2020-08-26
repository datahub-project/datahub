import { DatasetClassifiers } from 'datahub-web/constants';
import { arrayMap } from '@datahub/utils/array/index';
import { keysEquiv } from '@datahub/data-models/entity/dataset/helpers/validators/base';
import { IComplianceEntity } from 'datahub-web/typings/api/datasets/compliance';
import { IMetadataType } from '@datahub/data-models/types/entity/validators';

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
        '@name': 'readonly',
        '@type': 'boolean'
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

export default keysEquiv;

/**
 * Type guard asserts that object is assignable to { complianceEntities: Array<IComplianceEntity> }
 * @param {*} object object to be tested against complianceEntitiesTaxonomy
 * @returns {(object is { complianceEntities: Array<IComplianceEntity> })}
 */
const isMetadataObject = (object: unknown): object is { complianceEntities: Array<IComplianceEntity> } =>
  keysEquiv(object as Record<string, unknown>, complianceEntitiesTaxonomy);

export { complianceMetadataTaxonomy, complianceEntitiesTaxonomy, isMetadataObject };
