import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { waitUntil, find } from 'ember-native-dom-helpers';
import { urn } from 'wherehows-web/mirage/fixtures/urn';
import sinon from 'sinon';

moduleForComponent(
  'datasets/containers/dataset-health',
  'Integration | Component | datasets/containers/dataset-health',
  {
    integration: true
  }
);

test('it renders', async function(assert) {
  this.set('urn', urn);
  this.render(hbs`{{datasets/containers/dataset-health urn=urn}}`);

  assert.ok(find('.dataset-health__score-table'), 'renders the health table component');
});
