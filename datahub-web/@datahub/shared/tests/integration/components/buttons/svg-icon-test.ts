import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, click, triggerEvent, settled } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import sinon from 'sinon';
import { baseClass } from '@datahub/shared/components/buttons/svg-icon';

module('Integration | Component | buttons/svg-icon', function(hooks): void {
  setupRenderingTest(hooks);

  test('component rendering and attributes', async function(assert): Promise<void> {
    const title = 'title';
    const onClick = sinon.fake();
    const iconName = 'check-icon';

    await render(hbs`<Buttons::SvgIcon @title={{this.title}} @onClick={{this.onClick}} @iconName={{this.iconName}} />`);

    assert.dom().hasText('?', 'Expected a question mark to be rendered when an icon name is not provided');

    this.setProperties({ title, onClick });

    await triggerEvent(`.${baseClass}`, 'mouseenter');
    assert.dom('[role=tooltip]').isVisible();
    assert.dom('[role=tooltip]').hasText(title);

    await click(`.${baseClass}`);

    assert.ok(onClick.called, 'Expected onClick handler to be called when clicked');

    this.set('iconName', iconName);
    await settled();
    assert.dom('svg').exists();
  });

  test('forwarding DOM attributes', async function(assert): Promise<void> {
    const title = 'title';

    this.set('title', title);

    await render(hbs`<Buttons::SvgIcon title={{this.title}} disabled={{true}} />`);

    assert.dom('button').isDisabled('Expected the native HTML attribute disabled to be passed down');
    assert
      .dom('button')
      .hasAttribute('title', title, 'Expected native element attributes to override component attributes');
  });
});
