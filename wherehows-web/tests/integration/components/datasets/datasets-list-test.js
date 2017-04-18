import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('datasets/datasets-list', 'Integration | Component | datasets/datasets list', {
  integration: true
});

test('it renders', function(assert) {

  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });

  this.render(hbs`{{datasets/datasets-list}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:
  this.render(hbs`
    {{#datasets/datasets-list}}
      template block text
    {{/datasets/datasets-list}}
  `);

  assert.equal(this.$().text().trim(), 'template block text');
});
