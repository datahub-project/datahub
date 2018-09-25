import { arrayReduce } from 'wherehows-web/utils/array';
import buildUrl from 'wherehows-web/utils/build-url';
import { IMailHeaderRecord, MailerHeaderValue } from 'wherehows-web/typings/app/helpers/email';

/**
 * Constructs a `mailto:` address with supplied email headers as query parameters
 * @param {IMailHeaderRecord} [headers={}]
 * @returns {string}
 */
const buildMailToUrl = (headers: IMailHeaderRecord = {}): string => {
  const { to = '', ...otherHeaders } = headers;
  const [...otherHeaderPairs] = Object.entries(otherHeaders);
  const mailTo = `mailto:${to}`;

  return arrayReduce(
    (mailTo: string, [headerName, headerValue = '']: [string, MailerHeaderValue]) =>
      buildUrl(mailTo, headerName, String(headerValue)),
    mailTo
  )([...otherHeaderPairs]);
};

export { buildMailToUrl };
