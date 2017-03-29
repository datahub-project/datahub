import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('search/dropdown-selection', 'Integration | Component | search/dropdown selection', {
  integration: true
});

test('it renders', function(assert) {

  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });

  this.render(hbs`{{search/dropdown-selection}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:
  this.render(hbs`
    {{#search/dropdown-selection}}
      template block text
    {{/search/dropdown-selection}}
  `);

  assert.equal(this.$().text().trim(), 'template block text');
});
