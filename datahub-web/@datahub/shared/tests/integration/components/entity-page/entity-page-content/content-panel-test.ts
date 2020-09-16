import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { IContentPanelComponent, IFileViewerItem } from '@datahub/data-models/types/entity/rendering/page-components';

module('Integration | Component | entity-page/entity-page-content/content-panel', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    const entity: {
      file: IFileViewerItem;
    } = {
      file: {
        content: 'some text',
        type: 'text'
      }
    };
    const options: IContentPanelComponent<typeof entity>['options'] = {
      title: 'This is a title',
      contentComponent: {
        name: 'entity-page/entity-page-content/file-viewer',
        options: {
          emptyStateMessage: 'empty',
          propertyName: 'file'
        }
      }
    };
    this.setProperties({ options, entity });
    await render(
      hbs`<EntityPage::EntityPageContent::ContentPanel @options={{this.options}} @entity={{this.entity}} />`
    );

    assert.dom('.content-panel__title').hasText('This is a title');
    assert.dom('.ace_text-layer').hasText('some text');
  });
});
