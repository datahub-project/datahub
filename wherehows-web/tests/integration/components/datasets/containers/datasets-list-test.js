import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('datasets/containers/datasets-list', 'Integration | Component | datasets/containers/datasets list', {
  integration: true
});

test('it renders', function(assert) {

  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });

  this.render(hbs`{{datasets/containers/datasets-list}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:
  this.render(hbs`
    {{#datasets/containers/datasets-list}}
      template block text
    {{/datasets/containers/datasets-list}}
  `);

  assert.equal(this.$().text().trim(), 'template block text');
});
