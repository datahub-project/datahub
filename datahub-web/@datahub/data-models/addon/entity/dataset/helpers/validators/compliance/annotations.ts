import { IMetadataType } from '@datahub/data-models/types/entity/validators';
import { keysEquiv } from '@datahub/data-models/entity/dataset/helpers/validators/base';
import { IComplianceFieldAnnotation } from '@datahub/metadata-types/constants/entity/dataset/compliance-field-annotation';

export type ComplianceAnnotationsEditableProps =
  | 'identifierField'
  | 'identifierType'
  | 'logicalType'
  | 'isPurgeKey'
  | 'valuePattern'
  | 'isReadOnly';

export type RawComplianceAnnotationsEditableProps =
  | 'identifierField'
  | 'identifierType'
  | 'logicalType'
  | 'nonOwner'
  | 'valuePattern'
  | 'readonly';

/**
 * Lists the types for objects or instances in the the compliance metadata entities list
 * @type {Array<IMetadataType>}
 */
export const complianceAnnotationsTaxonomy: Array<IMetadataType> = [
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
        '@name': 'logicalType',
        '@type': ['string', 'null']
      },
      {
        '@name': 'isPurgeKey',
        '@type': ['boolean', 'null']
      },
      {
        '@name': 'valuePattern',
        '@type': ['string', 'null']
      },
      {
        '@name': 'isReadOnly',
        '@type': 'boolean'
      }
    ]
  }
];

/**
 * Lists the types for objects or instances in the the compliance metadata entities list
 * @type {Array<IMetadataType>}
 */
export const rawComplianceAnnotationsTaxonomy: Array<IMetadataType> = [
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
        '@name': 'logicalType',
        '@type': ['string', 'null']
      },
      {
        '@name': 'nonOwner',
        '@type': ['boolean', 'null']
      },
      {
        '@name': 'valuePattern',
        '@type': ['string', 'null']
      },
      {
        '@name': 'readonly',
        '@type': 'boolean'
      }
    ]
  }
];

/**
 * Type guard asserts that object is assignable to { complianceEntities: Array<ComplianceAnnotationsEditableProps> }
 * @param {*} object object to be tested against complianceEntitiesTaxonomy
 * @returns {(object is { complianceEntities: Array<Pick<DatasetComplianceAnnotation, ComplianceAnnotationsEditableProps>> })}
 */
export const isValidForEditableProps = (
  object: unknown
): object is Array<Pick<IComplianceFieldAnnotation, RawComplianceAnnotationsEditableProps>> =>
  keysEquiv({ complianceEntities: object }, rawComplianceAnnotationsTaxonomy);
