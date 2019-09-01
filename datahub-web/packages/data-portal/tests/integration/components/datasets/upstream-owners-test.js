import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { hdfsUrn } from 'wherehows-web/mirage/fixtures/urn';

module('Integration | Component | datasets/upstream owners', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    await render(hbs`{{datasets/upstream-owners}}`);

    assert.ok(document.querySelector('.upstream-owners-banner'), 'renders upstream-dataset as expected');
  });

  test('rendering nativeName', async function(assert) {
    const titleElementQuery = '.upstream-owners-banner__title strong';
    const nativeName = 'Upstream Dataset';

    this.set('nativeName', nativeName);

    await render(hbs`{{datasets/upstream-owners nativeName=nativeName}}`);

    assert.ok(
      document
        .querySelector(titleElementQuery)
        .textContent.trim()
        .includes(nativeName),
      'it renders upstream ownership properties'
    );
  });

  test('link to upstream dataset', async function(assert) {
    this.set('upstreamUrn', hdfsUrn);

    await render(hbs`{{datasets/upstream-owners upstreamUrn=upstreamUrn}}`);

    assert.ok(document.querySelector('.upstream-owners-banner a'), 'it creates a link to the upstream dataset');
  });
});
