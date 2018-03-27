import { IDropDownOption } from 'wherehows-web/constants/dataset-compliance';
import { AccessControlAccessType } from 'wherehows-web/utils/datasets/acl-access';

/**
 * Describes the interface for an AccessControlEntry object
 * @interface IAccessControlEntry
 */
export interface IAccessControlEntry {
  principal: string;
  accessType: Array<string>;
  businessJustification: string;
  expiresAt: number | null;
}

/**
 * Describes the interface for an AccessControl AccessType dropdown
 * @interface IAccessControlAccessTypeOption
 */
export interface IAccessControlAccessTypeOption extends IDropDownOption<AccessControlAccessType> {}

/**
 * Describes the interface for an IRequestAccessControlEntry object
 * @interface IRequestAccessControlEntry
 */
export type IRequestAccessControlEntry = Pick<IAccessControlEntry, 'businessJustification'> & {
  expiresAt?: IAccessControlEntry['expiresAt'];
  accessType: string;
};

export type IGetAclsResponse = Array<IAccessControlEntry>;
