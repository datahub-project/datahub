import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { IEntityPageContentContentPanelWithToggleArgs } from '@datahub/shared/components/entity-page/entity-page-content/content-panel-with-toggle';
import Component from '@ember/component';
import { ISharedStateArgs } from '@datahub/shared/types/entity-page/components/entity-page-content/content-panel-with-toggle';
import { action } from '@ember/object';

interface ISharedState {
  toggle: boolean;
}

class Toolbar extends Component implements ISharedStateArgs<ISharedState> {
  // start args
  state!: ISharedState;
  onStateChanged!: (state: ISharedState) => ISharedState;
  // end args

  layout = hbs`toolbar <button class="click-me" {{on 'click' (fn this.onClickMe)}}>someaction</button>`;

  @action
  onClickMe(): void {
    this.onStateChanged({ ...this.state, toggle: true });
  }
}

class Layout2 extends Component {
  layout = hbs`layout2 toggle: {{if this.state.toggle 'Yes' 'No'}}`;
}

class Layout1 extends Component {
  layout = hbs`layout1`;
}

module('Integration | Component | entity-page/entity-page-content/content-panel-with-toggle', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    this.owner.register('component:layout1', Layout1);
    this.owner.register('component:layout2', Layout2);
    this.owner.register('component:toolbar', Toolbar);

    const options: IEntityPageContentContentPanelWithToggleArgs['options'] = {
      title: 'Title',
      dropDownItems: [
        {
          label: 'Layout 1 button',
          value: {
            contentComponent: { name: 'layout1' }
          }
        },
        {
          label: 'Layout 2 button',
          value: {
            toolbarComponent: { name: 'toolbar' },
            contentComponent: { name: 'layout2' }
          }
        }
      ]
    };
    this.setProperties({ options, entity: { urn: 'aurn' } });
    await render(
      hbs`<EntityPage::EntityPageContent::ContentPanelWithToggle @entity={{this.entity}} @options={{this.options}} />`
    );

    assert.dom().containsText('layout1');

    await click('.nacho-drop-down__trigger');

    assert.dom().containsText('Layout 1 button');
    assert.dom().containsText('Layout 2 button');

    await click('.nacho-drop-down__options__option:nth-child(2)');

    assert.dom().doesNotContainText('Layout 1 button');

    assert.dom().doesNotContainText('layout1');
    assert.dom().containsText('layout2');
    assert.dom().containsText('toggle: No');
    assert.dom().containsText('toolbar');

    await click('.click-me');

    assert.dom().containsText('toggle: Yes');

    await click('.nacho-drop-down__trigger');
    await click('.nacho-drop-down__options__option:nth-child(1)');

    assert.dom().doesNotContainText('layout2');
    assert.dom().containsText('layout1');
  });
});
