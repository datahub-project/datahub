/**
 * Describes the interface about ACL authorized user's info
 */
interface IAclUserInfo {
  name: string;
  idType: string;
  source: string;
  modifiedTime: string;
  ownerShip: string;
  userName: string;
}

/**
 * Describes the interface for the body property in ACL response
 */
interface IRequestAclAccess {
  principal: string;
  businessJustification: string;
  accessTypes: Array<'READ' | 'WRITE'>;
  tableItem: IAclUserInfo;
  id?: string;
}

/**
 * Describe the interface which is the response from ACL permission request
 */
export interface IAclInfo {
  isAccess: boolean;
  body: Array<IRequestAclAccess>;
}

/**
 * Describe the interface for the rejected response from ACL authentication request
 */
export interface IRequestAclReject {
  isApproved: boolean;
}

/**
 * Describe the interface for the approved response from ACL authentication request
 */
export interface IRequestAclApproved {
  status: string;
  principal: string;
  businessJustification: string;
  accessTypes: Array<'READ' | 'WRITE'>;
  tableItem: {
    userName: string;
    name: string;
    idType: string;
    source: string;
    modifiedTime: string;
    ownerShip: string;
  };
}
/**
 * Describe the interface which is a response from ACL authentication request
 */
export type IRequestResponse = IRequestAclReject | IRequestAclApproved;

/**
 * Describe the interface to compose the ACL authentication request payload
 */
export interface Iprincipal {
  principal: string;
  businessJustification: string;
}

/**
 * Describe the interface for page static resources
 */
interface IpageInfo {
  info: string;
  requestInfo: string;
  requestMessage: string;
  classNameIcon: string;
  classNameFont: string;
}

/**
 * Describe the interface for page static resources in the authorization state and unauthorized state 
 */
export interface IpageConcent {
  success: IpageInfo;
  reject: IpageInfo;
}

/**
 * Describe the interface for the static page content in a state
 */
interface IpageStateInfo {
  state: string;
  info: string;
  icon: string;
  font: string;
  isLoadForm?: boolean;
  message?: string;
}

/**
 * Describe the interface for page content in each state
 */
export interface IpageState {
  [propName: string]: IpageStateInfo;
}
