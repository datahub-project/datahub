import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, click, waitUntil } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

const isElementInViewport = (el: Element | undefined | null, scrollableElement: Element): boolean => {
  if (!el) {
    return false;
  }

  const rect = el.getBoundingClientRect();
  const topOffset = scrollableElement.getBoundingClientRect().top;
  return rect.top - topOffset < 2; // We admit 2px off in scroll position as there is an animation and floating points
};
module('Integration | Component | link-to-section', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    assert.equal(this.element.textContent?.trim(), '');

    await render(hbs`
      <LinkToSection @sectionSelector=".something">
        template block text
      </LinkToSection>
      <div style="margin-top:4000px">margin</div>
      <div class="something">
        here
      </div>
      <div style="margin-top:4000px">margin</div>
    `);
    const scrollableElement = this.element.parentElement;
    const targetElement = this.element.querySelector('.something');

    if (scrollableElement) {
      assert.notOk(isElementInViewport(targetElement, scrollableElement), 'Element is not visible');
      await click('button');
      await waitUntil((): boolean => isElementInViewport(targetElement, scrollableElement), { timeout: 10000 });
      assert.ok(isElementInViewport(targetElement, scrollableElement), 'Element is visible');
    } else {
      assert.notOk(true, 'parent element not found');
    }
  });
});
