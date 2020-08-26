import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { IBrowseEntityModel } from '@datahub/shared/components/browser/containers/entity-categories';
import setupMirage from 'ember-cli-mirage/test-support/setup-mirage';
import { MockEntity } from '@datahub/data-models/entity/mock/mock-entity';
import { stubService } from '@datahub/utils/test-helpers/stub-service';
import { IEntityRenderProps } from '@datahub/data-models/types/entity/rendering/entity-render-props';

module('Integration | Component | browser/containers/entity-categories', function(hooks): void {
  setupRenderingTest(hooks);
  setupMirage(hooks);

  test('container component render & yielding', async function(assert): Promise<void> {
    assert.expect(2);

    class MyMockEntity extends MockEntity {
      static get renderProps(): IEntityRenderProps {
        return { ...MockEntity.renderProps };
      }
    }

    stubService('data-models', {
      getModel(_name: string) {
        return MyMockEntity;
      },
      createInstance(_entityDisplayName: string, urn: string): MockEntity {
        return new MyMockEntity(urn);
      }
    });

    const params: IBrowseEntityModel = {
      entity: MockEntity.displayName,
      page: 1,
      size: 1
    };

    this.set('params', params);

    await render(hbs`
      {{#browser/containers/entity-categories params=params as |entityContainer|}}
        <div id="entity-cat-nodes">
          {{entityContainer.browsePath.entities.length}}
        </div>
        <div id="entity-cat-definition">
          {{entityContainer.entityType.displayName}}
        </div>
      {{/browser/containers/entity-categories}}
    `);

    assert.equal(
      find('#entity-cat-nodes')?.textContent?.trim(),
      '0',
      'it should yield a nodes attribute with a length of 0'
    );
    assert.equal(
      find('#entity-cat-definition')?.textContent?.trim(),
      MockEntity.displayName,
      'it should yield an e with a DatamodelEntity property equal to the application defined constant'
    );
  });
});
