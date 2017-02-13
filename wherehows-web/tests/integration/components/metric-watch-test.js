import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('metric-watch', 'Integration | Component | metric watch', {
  integration: true
});

test('it renders', function(assert) {

  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });

  this.render(hbs`{{metric-watch}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:
  this.render(hbs`
    {{#metric-watch}}
      template block text
    {{/metric-watch}}
  `);

  assert.equal(this.$().text().trim(), 'template block text');
});
