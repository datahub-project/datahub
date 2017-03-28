import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('dataset-table-group-compliance', 'Integration | Component | dataset table group compliance', {
  integration: true
});

test('it renders', function(assert) {

  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });

  this.render(hbs`{{dataset-table-group-compliance}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:
  this.render(hbs`
    {{#dataset-table-group-compliance}}
      template block text
    {{/dataset-table-group-compliance}}
  `);

  assert.equal(this.$().text().trim(), 'template block text');
});
