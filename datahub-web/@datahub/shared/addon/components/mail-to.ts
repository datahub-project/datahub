import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../templates/components/mail-to';
import { layout, tagName, attribute } from '@ember-decorators/component';
import { computed } from '@ember/object';
import { buildMailToUrl, IMailHeaderRecord } from '@datahub/utils/helpers/email';

/**
 * This provides a way to render custom mailto: links in the DOM with custom attribute, such as a custom opener using the handler attribute.
 * A mailHeader is provided with attributes for email generation, and an optional handler
 * @export
 * @class MailTo
 * @extends {Component}
 */
@layout(template)
@tagName('a')
export default class MailTo extends Component {
  /**
   * Attributes to be used in email creation
   */
  mailHeader?: IMailHeaderRecord;

  /**
   * Optional handler for custom mailto link generation
   */
  handler = 'https://outlook.office.com/mail/deeplink/compose?mailtouri=';

  /**
   * Element attribute to open mail window in a new tab / window
   */
  @attribute
  target = '_blank';

  /**
   * Due to usage of target blank, adds 'noreferrer noopener' to rel attribute to prevent exploitation of the window.opener API
   */
  @attribute
  rel = 'noreferrer noopener';

  /**
   * Computes the anchor href from the supplied mailHeader attributes and other optional attributes
   * @readonly
   */
  @attribute
  @computed('mailToUrl')
  get href(): string {
    const { handler = '', mailToUrl } = this;
    return `${handler}${mailToUrl}`;
  }

  /**
   * Generates the mailto url from mail attributes
   * @readonly
   */
  @computed('mailHeader')
  get mailToUrl(): string {
    return buildMailToUrl(this.mailHeader);
  }
}
