import { Maybe } from 'wherehows-web/typings/app/core';

/**
 * Union string literal of safe email headers and the body field
 */
type MailerHeaders = 'to' | 'cc' | 'bcc' | 'subject' | 'body';

/**
 * Alias for a string of list of string email header values
 * @alias
 */
export type MailerHeaderValue = Maybe<string>;

/**
 * An optional record of email headers to it's value - MailerHeaderValue
 * @alias
 */
export type IMailHeaderRecord = Partial<Record<MailerHeaders, MailerHeaderValue>>;
