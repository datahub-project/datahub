import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('pendulum-ellipsis-animation', 'Integration | Component | pendulum ellipsis animation', {
  integration: true
});

test('it renders', function(assert) {
  this.render(hbs`{{pendulum-ellipsis-animation}}`);

  assert.equal(document.querySelector('.ellipsis-loader').tagName, 'DIV', 'renders a div with component class');
  assert.equal(document.querySelectorAll('.ellipsis-loader__circle').length, 3, 'contains three circle elements');
});
