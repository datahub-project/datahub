/**
 * Selector for wrapper containing elements for user login ui
 * @type {string}
 */
const loginContainer = '.wh-login-container';
/**
 * Url  / route for authentication
 * @type {string}
 */
const authenticationUrl = '/login';
/**
 * Expected error message for invalid credentials
 * @type {string}
 */
const invalidCredentials = 'Missing or invalid [credentials]';
/**
 * Test Username
 * @type {string}
 */
const testUser = 'testUser';
/**
 * Test User dummy password
 * @type {string}
 */
const testPassword = 'validPasswordWithNoVip';

/**
 * Test User dummy VIP TOKEN
 * @type {number}
 */
const testVipToken = 123456;

/**
 * Invalid password to trigger error message
 * @type {string}
 */
const testPasswordInvalid = 'invalidPassword';

export {
  loginContainer,
  authenticationUrl,
  invalidCredentials,
  testUser,
  testPassword,
  testVipToken,
  testPasswordInvalid
};
