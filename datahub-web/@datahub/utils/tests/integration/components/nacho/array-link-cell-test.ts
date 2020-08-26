import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { IDynamicLinkNode } from '@datahub/utils/types/vendor/dynamic-link';

module('Integration | Component | nacho/array-link-cell', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    const array: Array<IDynamicLinkNode<unknown, unknown>> = [
      {
        model: [''],
        queryParams: {},
        route: 'something',
        text: 'Text',
        title: 'link title attribute'
      }
    ];
    this.set('field', array);

    await render(hbs`<Nacho::ArrayLinkCell @field={{this.field}}/>`);

    assert.dom('.nacho-cell__field-value-list-item').hasText('Text');
  });
});
