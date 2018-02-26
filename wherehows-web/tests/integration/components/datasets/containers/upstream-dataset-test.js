import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { hdfsUrn } from 'wherehows-web/mirage/fixtures/urn';
import sinon from 'sinon';
import { waitUntil, find } from 'ember-native-dom-helpers';

moduleForComponent(
  'datasets/containers/upstream-dataset',
  'Integration | Component | datasets/containers/upstream dataset',
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
  assert.expect(2);
  const titleElementQuery = '.upstream-dataset-banner__title strong';
  const descriptionElementQuery = '.upstream-dataset-banner__description strong';

  this.set('upstreamUrn', hdfsUrn);
  this.server.respondWith(/\/api\/v2\/datasets.*/, function(xhr) {
    xhr.respond(
      200,
      { 'Content-Type': 'application/json' },
      JSON.stringify({
        dataset: {
          nativeName: 'A nativeName',
          description: 'A dataset description'
        }
      })
    );
  });

  this.render(hbs`{{datasets/containers/upstream-dataset upstreamUrn=upstreamUrn}}`);

  await waitUntil(() => find(descriptionElementQuery));

  assert.equal(find(titleElementQuery).textContent.trim(), 'A nativeName');
  assert.equal(find(descriptionElementQuery).textContent.trim(), 'A dataset description');
});
