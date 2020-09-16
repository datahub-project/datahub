import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import setupRouter from '@datahub/utils/test-helpers/setup-router';

module('Integration | Component | tracking/trackable-link-to', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    setupRouter(this);

    this.set('contentName', 'contentName');
    this.set('contentPiece', 'contentPiece');
    this.set('linkParams', { route: 'browse' });
    this.set('id', 'test-analytics-trackable-link-to');

    await render(hbs`
      <Tracking::TrackableLinkTo @params={{linkParams}} @contentPiece={{contentPiece}} @contentName={{contentName}} id={{id}}>
        Link to browse
      </Tracking::TrackableLinkTo>
    `);

    // For subsequent tests asserts that trackableLink is not null to prevent repeated checks, if false, tests will still fail as expected
    const trackableLink: HTMLElement | null = this.element.querySelector(
      '#test-analytics-trackable-link-to'
    ) as HTMLElement;

    assert.equal(trackableLink.tagName, 'A', 'expected component element to be an anchor tag');
    assert.equal(
      trackableLink.dataset.contentName,
      'contentName',
      'expected component to have a data-content-name attribute'
    );
    assert.equal(
      trackableLink.dataset.contentPiece,
      'contentPiece',
      'expected component to have a data-content-piece attribute'
    );

    assert.equal(trackableLink.getAttribute('href'), '/browse', 'expected the href attribute to be browse route');

    assert.ok(
      trackableLink.dataset.contentTarget?.includes('/browse'),
      'expected component attribute data-content-target to contain "browse"'
    );

    assert.equal(
      trackableLink.textContent && trackableLink.textContent.trim(),
      'Link to browse',
      'expected the anchor element text to match'
    );
  });
});
