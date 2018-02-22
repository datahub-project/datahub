import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { waitUntil, find } from 'ember-native-dom-helpers';
import { urn } from 'wherehows-web/mirage/fixtures/urn';
import sinon from 'sinon';

moduleForComponent(
  'datasets/containers/dataset-compliance',
  'Integration | Component | datasets/containers/dataset compliance',
  {
    integration: true,

    beforeEach() {
      this.server = sinon.createFakeServer();
    },

    afterEach() {
      this.server.restore();
    }
  }
);

test('it renders', async function(assert) {
  this.set('urn', urn);
  this.server.respondWith('GET', /\/api\/v2\/datasets\/.*/, [
    200,
    { 'Content-Type': 'application/json' },
    JSON.stringify({})
  ]);
  this.server.respondWith(/.*\/compliance-data-types/, [
    200,
    { 'Content-Type': 'application/json' },
    JSON.stringify([])
  ]);
  this.server.respondWith(/.*\/compliance\/suggestion/, [
    200,
    { 'Content-Type': 'application/json' },
    JSON.stringify({})
  ]);
  this.server.respondWith(/.*\/schema/, [
    200,
    { 'Content-Type': 'application/json' },
    JSON.stringify({
      schema: {
        schemaless: false,
        columns: [],
        rawSchema: null,
        keySchema: null
      }
    })
  ]);

  this.render(hbs`{{datasets/containers/dataset-compliance urn=urn}}`);
  this.server.respond();

  await waitUntil(() => find('.compliance-container'));
  assert.ok(document.querySelector('empty-state'), 'renders the empty state component');
});
