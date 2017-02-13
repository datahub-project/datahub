import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('dataset-favorite', 'Integration | Component | dataset favorite', {
  integration: true
});

test('it renders', function(assert) {

  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });

  this.render(hbs`{{dataset-favorite}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:
  this.render(hbs`
    {{#dataset-favorite}}
      template block text
    {{/dataset-favorite}}
  `);

  assert.equal(this.$().text().trim(), 'template block text');
});
