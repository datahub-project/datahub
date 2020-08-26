import buildUrl from '@datahub/utils/api/build-url';

/**
 * Union string literal of safe email headers and the body field
 */
type MailerHeaders = 'to' | 'cc' | 'bcc' | 'subject' | 'body';

/**
 * A dictionary of email headers to header values
 */
export type IMailHeaderRecord = Partial<Record<MailerHeaders, string>>;

/**
 * Constructs a `mailto:` address with supplied email headers as query parameters
 * @param {IMailHeaderRecord} [headers={}]
 */
export const buildMailToUrl = (headers: IMailHeaderRecord = {}): string => {
  const { to = '', ...remainingHeaders } = headers;
  const mailTo = `mailto:${encodeURIComponent(to)}`;

  return buildUrl(mailTo, remainingHeaders);
};
