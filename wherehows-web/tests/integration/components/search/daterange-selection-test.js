import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('search/daterange-selection', 'Integration | Component | search/daterange selection', {
  integration: true
});

test('it renders', function(assert) {

  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });

  this.render(hbs`{{search/daterange-selection}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:
  this.render(hbs`
    {{#search/daterange-selection}}
      template block text
    {{/search/daterange-selection}}
  `);

  assert.equal(this.$().text().trim(), 'template block text');
});
