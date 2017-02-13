import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('dataset-compliance', 'Integration | Component | dataset compliance', {
  integration: true
});

test('it renders', function(assert) {

  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });

  this.render(hbs`{{dataset-compliance}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:
  this.render(hbs`
    {{#dataset-compliance}}
      template block text
    {{/dataset-compliance}}
  `);

  assert.equal(this.$().text().trim(), 'template block text');
});
