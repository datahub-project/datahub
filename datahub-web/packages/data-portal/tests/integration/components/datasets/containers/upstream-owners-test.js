import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, waitUntil, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { hdfsUrn } from 'wherehows-web/mirage/fixtures/urn';
import sinon from 'sinon';

module('Integration | Component | datasets/containers/upstream owners', function(hooks) {
  setupRenderingTest(hooks);

  hooks.beforeEach(function() {
    this.sinonServer = sinon.createFakeServer();
    this.sinonServer.respondImmediately = true;
  });

  hooks.afterEach(function() {
    this.sinonServer.restore();
  });

  test('it renders', async function(assert) {
    const titleElementQuery = '.upstream-owners-banner__title strong';
    const nativeName = 'A nativeName';

    this.set('upstreamUrn', hdfsUrn);
    this.sinonServer.respondWith(/\/api\/v2\/datasets.*/, function(xhr) {
      xhr.respond(
        200,
        { 'Content-Type': 'application/json' },
        JSON.stringify({
          dataset: {
            nativeName
          }
        })
      );
    });

    await render(hbs`{{datasets/containers/upstream-owners upstreamUrn=upstreamUrn}}`);

    await waitUntil(() => find(titleElementQuery));

    assert.equal(find(titleElementQuery).textContent.trim(), nativeName);
  });
});
