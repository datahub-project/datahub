import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { find } from 'ember-native-dom-helpers';
import { urn } from 'wherehows-web/mirage/fixtures/urn';
import sinon from 'sinon';

moduleForComponent(
  'datasets/containers/dataset-schema',
  'Integration | Component | datasets/containers/dataset schema',
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

test('it renders', function(assert) {
  this.set('urn', urn);
  this.render(hbs`{{datasets/containers/dataset-schema urn=urn}}`);

  assert.ok(find('#json-viewer'), 'renders the dataset schema component');
});
