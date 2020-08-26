import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { IFileViewerComponent, IFileViewerItem } from '@datahub/data-models/types/entity/rendering/page-components';

module('Integration | Component | entity-page/entity-page-content/json-viewer', function(hooks): void {
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
    const options: IFileViewerComponent<typeof entity>['options'] = {
      emptyStateMessage: 'empty',
      propertyName: 'file'
    };

    this.setProperties({ options, entity });
    await render(hbs`<EntityPage::EntityPageContent::FileViewer @options={{this.options}} @entity={{this.entity}} />`);

    assert.dom('.ace_text-layer').hasText('some text');

    this.setProperties({ options, entity: {} });

    assert.dom('.empty-state').hasText('empty');

    this.setProperties({
      options,
      entity: {
        file: {
          content: '{ "property": "value" }',
          type: 'json'
        }
      }
    });
    assert.dom('.ace_variable').hasText('"property"');
    assert.dom('.ace_string').hasText('"value"');

    this.setProperties({
      options,
      entity: {
        file: {
          content: 'type Meta { count: Int }',
          type: 'graphqlschema'
        }
      }
    });
    assert.dom('.ace_keyword').hasText('type');
    assert.dom('.ace_identifier').hasText('Meta');
    assert.dom('.ace_storage').hasText('Int');
  });
});
