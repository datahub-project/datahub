import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

const elementQuery = '.more-info';

module('Integration | Component | more info', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    await render(hbs`{{more-info}}`);
    const element = document.querySelector(elementQuery) as Element;

    assert.ok(element, 'component is rendered in the DOM');
    assert.equal(
      (element.querySelector('a') as Element).getAttribute('target'),
      '_blank',
      'sets the default target attribute when none is provided'
    );
    assert.equal(
      (element.querySelector('a') as Element).getAttribute('href'),
      '#',
      'it sets a default href attribute when none is provided'
    );
  });

  test('MoreInfo', async function(assert): Promise<void> {
    const externalUrl = 'https://www.linkedin.com';
    const target = '_self';

    this.set('href', externalUrl);
    this.set('target', target);

    await render(hbs`{{more-info target=target link=href}}`);
    const element = document.querySelector(elementQuery) as Element;

    assert.equal(
      (element.querySelector('a') as Element).getAttribute('target'),
      target,
      'it sets the passed in target attribute when on is provided'
    );
    assert.equal(
      (element.querySelector('a') as Element).getAttribute('href'),
      externalUrl,
      'it sets the passed href attribute when a value is provided'
    );
  });
});
