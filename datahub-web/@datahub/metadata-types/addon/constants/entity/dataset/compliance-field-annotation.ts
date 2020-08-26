import {
  ComplianceFieldIdValue,
  NonMemberIdLogicalType,
  MemberIdLogicalType
} from '@datahub/metadata-types/constants/entity/dataset/compliance-field-types';

/**
 * The basic interface level definition for a compliance annotation tag. This should primarily be used only be the
 * DatasetComplianceAnnotation class and that is used externally to determine interface.
 * TODO: META-10942 Relocate to @linkedin/metadata-types
 * @export
 * @namespace Dataset
 */
export interface IComplianceFieldAnnotation {
  identifierField: string;
  identifierType?: ComplianceFieldIdValue | NonMemberIdLogicalType | string | null;
  logicalType?: MemberIdLogicalType | null;
  nonOwner?: boolean | null;
  pii: boolean;
  readonly?: boolean;
  securityClassification: string | null;
  valuePattern?: string | null;
}
