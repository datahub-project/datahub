import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { IDynamicLinkNode } from '@datahub/utils/types/vendor/dynamic-link';

module('Integration | Component | properties/single-value-renderer', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    await render(hbs`<Properties::SingleValueRenderer @value="hola" />`);

    assert.dom().hasText('hola');
  });

  test('it renders with link', async function(assert): Promise<void> {
    const link: IDynamicLinkNode<unknown, unknown> = {
      model: [''],
      queryParams: {},
      route: 'something',
      text: 'Text',
      title: 'link title attribute'
    };
    this.setProperties({ link, dynamicComponent: { name: 'link/optional-value' } });
    await render(hbs`<Properties::SingleValueRenderer @value={{this.link}} @component={{this.dynamicComponent}}/>`);

    assert.dom().hasText('Text');
  });

  test('it renders with boolean', async function(assert): Promise<void> {
    await render(hbs`<Properties::SingleValueRenderer @value={{true}} />`);

    assert.dom().hasText('Yes');

    await render(hbs`<Properties::SingleValueRenderer @value={{false}} />`);

    assert.dom().hasText('No');
  });
});
