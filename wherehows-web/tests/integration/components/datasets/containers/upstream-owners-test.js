import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { hdfsUrn } from 'wherehows-web/mirage/fixtures/urn';
import sinon from 'sinon';
import { waitUntil, find } from 'ember-native-dom-helpers';

module('Integration | Component | datasets/containers/upstream owners', function(hooks) {
  setupRenderingTest(hooks);

  hooks.beforeEach(function() {
    this.server = sinon.createFakeServer();
    this.server.respondImmediately = true;
  });

  hooks.afterEach(function() {
    this.server.restore();
  });

  test('it renders', async function(assert) {
    const titleElementQuery = '.upstream-owners-banner__title strong';
    const nativeName = 'A nativeName';

    this.set('upstreamUrn', hdfsUrn);
    this.server.respondWith(/\/api\/v2\/datasets.*/, async function(xhr) {
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
