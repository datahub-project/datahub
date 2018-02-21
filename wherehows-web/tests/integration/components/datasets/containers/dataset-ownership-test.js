import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { waitUntil, find } from 'ember-native-dom-helpers';
import { urn } from 'wherehows-web/mirage/fixtures/urn';
import sinon from 'sinon';

moduleForComponent(
  'datasets/containers/dataset-ownership',
  'Integration | Component | datasets/containers/dataset ownership',
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
  const lookupClass = '.dataset-author-user-lookup';
  this.set('urn', urn);
  this.server.respondWith('GET', /\/api\/v2\/datasets.*/, [
    200,
    { 'Content-Type': 'application/json' },
    JSON.stringify({})
  ]);
  this.server.respondWith('GET', '/api/v1/owner/types', [
    200,
    { 'Content-Type': 'application/json' },
    JSON.stringify([])
  ]);

  this.render(hbs`{{datasets/containers/dataset-ownership urn=urn}}`);

  await waitUntil(() => find(lookupClass));
  assert.equal(
    document.querySelector(lookupClass).textContent.trim(),
    'Add an Owner',
    'shows dataset authors component'
  );
});
