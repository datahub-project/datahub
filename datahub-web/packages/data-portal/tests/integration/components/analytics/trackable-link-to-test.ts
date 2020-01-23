import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import setupRouter from 'wherehows-web/tests/helpers/setup-router';

module('Integration | Component | analytics/trackable-link-to', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    setupRouter(this);

    this.set('contentName', 'contentName');
    this.set('contentPiece', 'contentPiece');
    this.set('link', 'search');
    this.set('id', 'test-analytics-trackable-link-to');

    await render(hbs`
      {{#analytics/trackable-link-to link contentPiece=contentPiece contentName=contentName id=id}}
        Link to Search
      {{/analytics/trackable-link-to}}
    `);

    const trackableLink: HTMLElement | null = this.element.querySelector('#test-analytics-trackable-link-to');

    assert.equal(trackableLink!.tagName, 'A', 'expected component element to be an anchor tag');
    assert.equal(
      trackableLink!.dataset.contentName,
      'contentName',
      'expected component to have a data-content-name attribute'
    );
    assert.equal(
      trackableLink!.dataset.contentPiece,
      'contentPiece',
      'expected component to have a data-content-piece attribute'
    );

    assert.equal(trackableLink!.getAttribute('href'), '/search', 'expected the href attribute to be search route');

    assert.ok(
      trackableLink!.dataset.contentTarget!.includes('/search'),
      'expected component attribute data-content-target to contain "search"'
    );

    assert.equal(trackableLink!.textContent!.trim(), 'Link to Search', 'expected the anchor element text to match');
  });
});
