import { ApiStatus } from 'wherehows-web/utils/api/shared';
import { Response, faker } from 'ember-cli-mirage';

const textContentHeader = { 'Content-Type': 'text/plain; charset=utf-8' };
//TODO: Build a dummy server to simulate ACL authentication process
/**
 * Build response body property
 * @param {sting} ldap 
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
  const { principal, businessJustification } = <{
    principal: string;
    businessJustification: string;
  }>JSON.parse(requestBody);

  const isApproved = businessJustification.toLowerCase().includes('read');

  if (businessJustification && isApproved) {
    let LADA: string | undefined = principal.split(':').get('lastObject');
    const responseBody = {
      principal,
      businessJustification,
      accessTypes: 'READ',
      ...approvedUserInfo(LADA)
    };
    _schema.db.datasetAclUsers.insert(responseBody);
    return {
      status: ApiStatus.OK,
      ...responseBody
    };
  }
  return new Response(401, textContentHeader, { status: ApiStatus.FAILED, isApproved: false });
};

export { aclAuth };
