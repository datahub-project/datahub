import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { IDynamicLinkNode } from '@datahub/utils/types/vendor/dynamic-link';

module('Integration | Component | properties/value-renderer', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    const array: Array<IDynamicLinkNode<unknown, unknown> | string> = [
      {
        model: [''],
        queryParams: {},
        route: 'something',
        text: 'Text1',
        title: 'link title attribute'
      },
      'Text2'
    ];
    this.setProperties({ array, dynamicComponent: { name: 'link/optional-value' } });

    await render(hbs`<Properties::ValueRenderer @value={{this.array}} @component={{this.dynamicComponent}}/>`);

    assert.dom().hasText('Text1 Text2');
  });
});
