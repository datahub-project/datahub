import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | dynamic-link-with-action', function(hooks): void {
  setupRenderingTest(hooks);

  // Since the dynamic link component comes from an addon, we only care about the part we extended
  test('extended functionality works as intended', async function(assert): Promise<void> {
    await render(hbs`<DynamicLinkWithAction />`);
    assert.ok(this.element, 'Initial render is without errors');

    let functionCalled = 0;
    this.set('myAction', () => {
      functionCalled += 1;
    });

    await render(hbs`<DynamicLinkWithAction @onClickAction={{myAction}} class="testLink" />`);
    assert.dom('.testLink').exists();
    await click('.testLink');
    assert.equal(functionCalled, 1, 'Function is expected to have only been called once');
  });
});
