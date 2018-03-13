import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { hdfsUrn } from 'wherehows-web/mirage/fixtures/urn';
import sinon from 'sinon';
import { waitUntil, find } from 'ember-native-dom-helpers';

moduleForComponent(
  'datasets/containers/upstream-owners',
  'Integration | Component | datasets/containers/upstream owners',
  {
    integration: true,

    beforeEach() {
      this.server = sinon.createFakeServer();
      this.server.respondImmediately = true;
    },

    afterEach() {
      this.server.restore();
    }
  }
);

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

  this.render(hbs`{{datasets/containers/upstream-owners upstreamUrn=upstreamUrn}}`);

  await waitUntil(() => find(titleElementQuery));

  assert.equal(find(titleElementQuery).textContent.trim(), nativeName);
});
