import {
  ComplianceFieldIdValue,
  NonMemberIdLogicalType
} from '@datahub/metadata-types/constants/entity/dataset/compliance-field-types';
import { INachoDropdownOption } from '@nacho-ui/core/types/nacho-dropdown';

/**
 * Defines the interface for compliance data type field option
 * @interface IComplianceFieldIdentifierOption
 * @extends {INachoDropdownOption<ComplianceFieldIdValue>}
 */
export interface IComplianceFieldIdentifierOption
  extends INachoDropdownOption<ComplianceFieldIdValue | NonMemberIdLogicalType> {
  isId: boolean;
}
