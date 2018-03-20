import {
  IAccessControlAccessTypeOption,
  IRequestAccessControlEntry
} from 'wherehows-web/typings/api/datasets/aclaccess';
import { arrayMap } from 'wherehows-web/utils/array';
import { formatAsCapitalizedStringWithSpaces } from 'wherehows-web/utils/helpers/string';

/**
 * Enumerates available access control access types
 */
enum AccessControlAccessType {
  Read = 'READ',
  Write = 'WRITE',
  ReadWrite = 'READ_WRITE'
}

/**
 * Creates the list of access control dropdown options
 * @param {Array<AccessControlAccessType>} accessTypes
 * @return {Array<IAccessControlAccessTypeOption>}
 */
const getAccessControlTypeOptions = (
  accessTypes: Array<AccessControlAccessType>
): Array<IAccessControlAccessTypeOption> => {
  const getTypeOption = (value: AccessControlAccessType): IAccessControlAccessTypeOption => ({
    value,
    label: formatAsCapitalizedStringWithSpaces(value)
  });
  return arrayMap(getTypeOption)(accessTypes);
};

/**
 * Returns a default object for access control entry object
 * @return {IRequestAccessControlEntry}
 */
const getDefaultRequestAccessControlEntry = (): IRequestAccessControlEntry => ({
  accessType: AccessControlAccessType.Read,
  businessJustification: ''
});

export { getAccessControlTypeOptions, AccessControlAccessType, getDefaultRequestAccessControlEntry };
