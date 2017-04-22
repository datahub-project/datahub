import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('browser/containers/browser-view', 'Integration | Component | browser/containers/browser view', {
  integration: true
});

test('it renders', function(assert) {

  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });

  this.render(hbs`{{browser/containers/browser-view}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:
  this.render(hbs`
    {{#browser/containers/browser-view}}
      template block text
    {{/browser/containers/browser-view}}
  `);

  assert.equal(this.$().text().trim(), 'template block text');
});
