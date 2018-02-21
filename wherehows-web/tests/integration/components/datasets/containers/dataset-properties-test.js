import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { waitUntil, find } from 'ember-native-dom-helpers';
import { urn } from 'wherehows-web/mirage/fixtures/urn';

moduleForComponent(
  'datasets/containers/dataset-properties',
  'Integration | Component | datasets/containers/dataset properties',
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

test('it renders', function(assert) {
  const labelClass = '.dataset-deprecation-toggle__toggle-header__label';
  this.set('urn', urn);
  this.server.respondWith('GET', /\/api\/v2\/datasets.*/, [
    200,
    { 'Content-Type': 'application/json' },
    JSON.stringify({})
  ]);

  this.render(hbs`{{datasets/containers/dataset-properties urn=urn}}`);

  assert.equal(find(labelClass).textContent.trim(), 'Is this dataset deprecated?', 'renders presentation component');
});
