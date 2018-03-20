import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent(
  'datasets/containers/dataset-acl-access',
  'Integration | Component | datasets/containers/dataset acl access',
  {
    integration: true
  }
);

skip('it renders', function(assert) {
  this.render(hbs`{{datasets/containers/dataset-acl-access}}`);

  assert.equal(
    this.$()
      .text()
      .trim(),
    ''
  );
});
