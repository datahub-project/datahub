import { ApiStatus } from '@datahub/utils/api/shared';
import { Response, faker } from 'ember-cli-mirage';
import ApprovedAclUser from 'wherehows-web/mirage/fixtures/dataset-acl-users';

const textContentHeader = { 'Content-Type': 'text/plain; charset=utf-8' };
//TODO: Build a dummy server to simulate ACL authentication process
/**
 * Build response body property
 * @param {string} ldap
 */
const approvedUserInfo = (ldap: string | undefined) => {
  return {
    tableItem: {
      userName: ldap,
      name: faker.name.findName(),
      idType: 'USER',
      source: `${faker.random.alphaNumeric()}${faker.random.alphaNumeric()}`.toUpperCase(),
      modifiedTime: faker.date.past(),
      ownerShip: 'DataOwner'
    }
  };
};

/**
 * Build ACL authentication server POST logic
 * @param {any}_schema
 * @param {string} requestBody
 */
const aclAuth = (_schema: any, { requestBody }: { requestBody: string }) => {
  const { principal, businessJustification } = <
    {
      principal: string;
      businessJustification: string;
    }
  >JSON.parse(requestBody);

  const isApproved = businessJustification.toLowerCase().includes('read');

  if (businessJustification && isApproved) {
    const principalLdap = principal.split(':').pop();
    const responseBody = {
      principal,
      businessJustification,
      accessTypes: 'READ',
      ...approvedUserInfo(principalLdap)
    };
    _schema.db.datasetAclUsers.insert(responseBody);
    return {
      status: ApiStatus.OK,
      ...responseBody
    };
  }
  return new Response(401, textContentHeader, { status: ApiStatus.FAILED, isApproved: false });
};

/**
 *  For testing purpose, defined a mocked response when user is granted ACL permission
 */
const approvedResponseTesting = ApprovedAclUser[0];

/**
 * For testing purpose, defined a method to mock ACL server response
 */
const accessInfoTesting = (permmision: boolean) => {
  return {
    isAccess: permmision,
    body: [approvedResponseTesting]
  };
};

export { aclAuth, accessInfoTesting, approvedResponseTesting };
