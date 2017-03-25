import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('search/checkbox-group', 'Integration | Component | search/checkbox group', {
  integration: true
});

test('it renders', function(assert) {

  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });

  this.render(hbs`{{search/checkbox-group}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:
  this.render(hbs`
    {{#search/checkbox-group}}
      template block text
    {{/search/checkbox-group}}
  `);

  assert.equal(this.$().text().trim(), 'template block text');
});
