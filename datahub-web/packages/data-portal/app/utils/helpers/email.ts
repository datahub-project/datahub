import buildUrl from 'wherehows-web/utils/build-url';
import { IMailHeaderRecord } from 'wherehows-web/typings/app/helpers/email';

/**
 * Constructs a `mailto:` address with supplied email headers as query parameters
 * @param {IMailHeaderRecord} [headers={}]
 * @returns {string}
 */
const buildMailToUrl = (headers: IMailHeaderRecord = {}): string => {
  const { to = '', ...otherHeaders } = headers;
  const mailTo = `mailto:${encodeURIComponent(to)}`;

  return buildUrl(mailTo, otherHeaders);
};

export { buildMailToUrl };
