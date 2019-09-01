import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | pendulum ellipsis animation', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    await render(hbs`{{pendulum-ellipsis-animation}}`);

    assert.equal(document.querySelector('.ellipsis-loader').tagName, 'DIV', 'renders a div with component class');
    assert.equal(document.querySelectorAll('.ellipsis-loader__circle').length, 3, 'contains three circle elements');
  });
});
