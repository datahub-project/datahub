// Constant value for the CSRF protection cookie name
export const csrfCookieName = 'datahub-csrf-token';

/**
 * Builds a regular expression pattern and object using the provided cookie name
 * @param {string} cookieName The name of the cookie to match
 */
const buildCookieRegExp = (cookieName: string): RegExp => new RegExp(cookieName + '="?([^";]+)"?');

/**
 * Uses the provided regular expression object to parse the Document cookie property for a
 * @param {RegExp} cookieRegExp A RegExp object instance to match against when parsing the cookie string
 */
const getCookieValue = (cookieRegExp: RegExp): string => {
  const [, token = ''] = document.cookie.match(cookieRegExp) || [];

  return token;
};

/**
 * Stored reference to the default cookie regular expression for CSRF tokens
 */
const defaultCSRFCookieRegExp = buildCookieRegExp(csrfCookieName);

/**
 * Retrieves the csrf token string from the document.cookie string
 * @export
 * @returns {string}
 */
export default function getCSRFToken(optionalTokenRegEx: RegExp = defaultCSRFCookieRegExp): string {
  return getCookieValue(optionalTokenRegEx);
}
