import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { IMailHeaderRecord } from '@datahub/utils/helpers/email';

module('Integration | Component | mail-to', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    const noRecipient = 'No email found';
    let mailHeader: IMailHeaderRecord = {};

    this.setProperties({ mailHeader });

    await render(hbs`
      <MailTo />
    `);

    assert.dom(this.element).hasText('No email found', `Expected rendered text to be ${noRecipient}`);
    assert.dom('a').exists('Expected an anchor element to be rendered in the DOM');
    assert.dom('a').hasAttribute('rel', 'noreferrer noopener');
    assert.dom('a').hasAttribute('target', '_blank');

    await render(hbs`
      <MailTo @mailHeader={{this.mailHeader}} @handler="" />
    `);

    assert
      .dom(this.element)
      .hasText('No email found', `Expected ${noRecipient} to be rendered text when an empty mail header is found`);

    mailHeader = { ...mailHeader, to: 'email@example.com' };
    this.set('mailHeader', mailHeader);
    assert.dom('a').hasAttribute('href', 'mailto:email%40example.com');

    mailHeader = { ...mailHeader, subject: 'An email subject' };
    this.set('mailHeader', mailHeader);
    assert.dom('a').hasAttribute('href', 'mailto:email%40example.com?subject=An+email+subject');

    mailHeader = { ...mailHeader, cc: 'A carbon copy' };
    this.set('mailHeader', mailHeader);
    assert.dom('a').hasAttribute('href', 'mailto:email%40example.com?subject=An+email+subject&cc=A+carbon+copy');

    mailHeader = { ...mailHeader, bcc: 'A blind carbon copy' };
    this.set('mailHeader', mailHeader);
    assert
      .dom('a')
      .hasAttribute(
        'href',
        'mailto:email%40example.com?subject=An+email+subject&cc=A+carbon+copy&bcc=A+blind+carbon+copy'
      );

    await render(hbs`
      <MailTo @mailHeader={{this.mailHeader}} @handler="mailtourihandler=" />
    `);

    assert
      .dom('a')
      .hasAttribute(
        'href',
        'mailtourihandler=mailto:email%40example.com?subject=An+email+subject&cc=A+carbon+copy&bcc=A+blind+carbon+copy'
      );
  });
});
