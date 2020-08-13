import { helper } from '@ember/component/helper';
import { ComplianceFieldIdValue } from '@datahub/metadata-types/constants/entity/dataset/compliance-field-types';

/**
 * Returns whether a passed in value is a none annotation tag
 * @param value - passed in value
 */
export function complianceIsNone([value]: [unknown]): boolean {
  return value === ComplianceFieldIdValue.None;
}

export default helper(complianceIsNone);
