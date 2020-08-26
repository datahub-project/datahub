import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find, findAll } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

const animationClass = '.nacho-ellipsis-animation';

module('Integration | Component | nacho-animation/pendulum-ellipsis', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    await render(hbs`{{nacho-animation/pendulum-ellipsis}}`);
    assert.ok(this.element, 'Initial render without errors');
    assert.equal((find(animationClass) as Element).tagName, 'DIV', "Renders a div with the component's class");
    assert.equal(findAll(`${animationClass}__circle`).length, 3, 'Contains three circle elements');
  });
});
