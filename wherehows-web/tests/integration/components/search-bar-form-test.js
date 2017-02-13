import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('search-bar-form', 'Integration | Component | search bar form', {
  integration: true
});

test('it renders', function(assert) {

  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });

  this.render(hbs`{{search-bar-form}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:
  this.render(hbs`
    {{#search-bar-form}}
      template block text
    {{/search-bar-form}}
  `);

  assert.equal(this.$().text().trim(), 'template block text');
});
