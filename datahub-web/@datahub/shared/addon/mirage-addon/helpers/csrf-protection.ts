import { csrfCookieName } from '@datahub/utils/helpers/csrf-token';
import { v4 as getUuid } from 'ember-uuid';

/**
 * Augments the document cookie object with the CSRF protection cookie
 */
export const setCSRFCookieToken = (): string => {
  const currentCookieState = document.cookie;
  const csrfCookie = `${csrfCookieName}=${getUuid()}; `;
  document.cookie = currentCookieState + csrfCookie;

  return csrfCookie;
};
