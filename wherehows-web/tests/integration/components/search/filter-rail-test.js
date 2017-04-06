import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('search/filter-rail', 'Integration | Component | search/filter rail', {
  integration: true
});

test('it renders', function(assert) {

  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });

  this.render(hbs`{{search/filter-rail}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:
  this.render(hbs`
    {{#search/filter-rail}}
      template block text
    {{/search/filter-rail}}
  `);

  assert.equal(this.$().text().trim(), 'template block text');
});
