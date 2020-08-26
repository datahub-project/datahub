import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { ITabProperties } from '@datahub/data-models/types/entity/rendering/entity-render-props';
import { modalClass } from '@datahub/shared/components/tab-content-modal';

module('Integration | Component | tab-content-modal', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders and tabs behave correctly', async function(assert): Promise<void> {
    const tabs: Array<ITabProperties> = [
      {
        id: 'tab1',
        title: 'Sample Title 1',
        contentComponent: 'blank-template'
      },
      {
        id: 'tab2',
        title: 'Sample Title 2',
        contentComponent: 'blank-template'
      }
    ];

    this.setProperties({
      tabs,
      tabSelected: 'tab1'
    });

    await render(hbs`
      <TabContentModal
        @tabs={{this.tabs}}
        @tabSelected={this.tabSelected}}
      />
        <span class="selected-tab-content">
          {{this.tabSelected}}
        </span>
    `);

    const modalClassSelector = `.${modalClass}`;
    const tabListClassSelector = `${modalClassSelector}__tab-list`;
    const activeTabClassSelector = '.selected-tab-content';

    assert.dom(modalClassSelector).exists();
    assert.equal(
      document.querySelector(tabListClassSelector)?.childElementCount,
      tabs.length,
      'Expected the number of tabs rendered to be equal to the number of tab properties'
    );

    // Tab content should be reflected by the tab selected
    assert.dom(activeTabClassSelector).hasText('tab1');

    // Changing the tab selected attribute should the respective tab content
    this.set('tabSelected', 'tab2');
    assert.dom(activeTabClassSelector).hasText('tab2');
  });
});
