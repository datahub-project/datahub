import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | pendulum ellipsis animation', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    await render(hbs`<NachoAnimation::PendulumEllipsis />`);

    assert.equal(
      document.querySelector('.nacho-ellipsis-animation')?.tagName,
      'DIV',
      'renders a div with component class'
    );
    assert.equal(
      document.querySelectorAll('.nacho-ellipsis-animation__circle').length,
      3,
      'contains three circle elements'
    );
  });
});
