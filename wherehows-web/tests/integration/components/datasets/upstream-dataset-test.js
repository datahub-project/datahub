import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { hdfsUrn, nonHdfsUrn } from 'wherehows-web/mirage/fixtures/urn';

module('Integration | Component | datasets/upstream dataset', function(hooks) {
  setupRenderingTest(hooks);
  test('it renders', async function(assert) {
    await render(hbs`{{datasets/upstream-dataset}}`);

    assert.ok(document.querySelector('.upstream-downstream-retention'), 'renders upstream-dataset as expected');
  });

  test('it renders upstream dataset properties', async function(assert) {
    const upstreamLink = '.upstream-dataset__upstream-link';
    const upstreamsMetadata = [{ dataset: {} }, { dataset: {} }];
    this.set('upstreamsMetadata', upstreamsMetadata);
    await render(hbs`{{datasets/upstream-dataset upstreamsMetadata=upstreamsMetadata}}`);

    assert.equal(document.querySelectorAll(upstreamLink).length, 2, 'renders a link for each upstream dataset');
  });
});
