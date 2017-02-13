import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('metric-detail', 'Integration | Component | metric detail', {
  integration: true
});

test('it renders', function(assert) {

  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });

  this.render(hbs`{{metric-detail}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:
  this.render(hbs`
    {{#metric-detail}}
      template block text
    {{/metric-detail}}
  `);

  assert.equal(this.$().text().trim(), 'template block text');
});
