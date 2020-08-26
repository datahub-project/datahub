import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { getRenderedComponent } from '@datahub/utils/test-helpers/register-component';
import FormsActionDrawer from '@datahub/shared/components/forms/action-drawer';
import { click } from '@ember/test-helpers';

module('Integration | Component | forms/action-drawer', function(hooks): void {
  setupRenderingTest(hooks);

  test('rendering and behavior', async function(assert): Promise<void> {
    const component = await getRenderedComponent({
      ComponentToRender: FormsActionDrawer,
      testContext: this,
      componentName: 'forms/action-drawer',
      template: hbs`
        <Forms::ActionDrawer as |drawer|>
        <button {{on "click" (fn drawer.onDrawerToggle)}} style="height: 18px" type="button">Click Me!</button>
        <div style="height: 200px;"></div>
        </Forms::ActionDrawer>
      `
    });
    const componentBaseClassSelector = `.${component.baseClass}`;

    assert.notOk(component.isExpanded, 'Expected component to be collapsed on first render');
    assert
      .dom(componentBaseClassSelector)
      .hasStyle({ height: '64px', position: 'fixed', right: '0px', bottom: '0px', left: '0px' });
    assert.dom(componentBaseClassSelector).hasClass(`${component.baseClass}--close`);
    assert.dom().hasText('Click Me!');

    await click('button');

    assert.ok(component.isExpanded, 'Expected component to be expanded');
    assert.dom(componentBaseClassSelector).hasClass(`${component.baseClass}--open`);

    const el = document.querySelector(componentBaseClassSelector);
    // button 18px + border 1px + inserted div 200px
    assert.ok(
      el && parseInt(window.getComputedStyle(el).height || '0', 10) > 200,
      'Expected element height to be greater than nested div height'
    );
  });
});
