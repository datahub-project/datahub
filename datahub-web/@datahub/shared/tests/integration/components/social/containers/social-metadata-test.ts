import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find, findAll, waitFor } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { stubService } from '@datahub/utils/test-helpers/stub-service';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { baseSocialMetadataComponentClass } from '@datahub/shared/components/social/containers/social-metadata';
import { setAspect } from '@datahub/data-models/entity/utils/aspects';

module('Integration | Component | social/containers/social-metadata', function(hooks): void {
  setupRenderingTest(hooks);

  const baseSelector = `.${baseSocialMetadataComponentClass}`;

  let datasetEntity: DatasetEntity;

  hooks.beforeEach(function(): void {
    datasetEntity = new DatasetEntity('pikachu');
    datasetEntity.readLikes = function(): Promise<void> {
      // TODO: [META-11037] Use mirage instead of hardcode for like actions
      setAspect(datasetEntity, 'likes', {
        actions: [{ likedBy: 'aketchum' }, { likedBy: 'misty' }, { likedBy: 'brock' }]
      });
      return new Promise((res): void => res());
    };

    datasetEntity.readFollows = function(): Promise<void> {
      // TODO: [META-11037] Use mirage instead of hardcode for like actions
      setAspect(datasetEntity, 'follow', { followers: [{ follower: { corpUser: 'aketchum' } }] });
      return new Promise((res): void => res());
    };
  });

  test('it renders and behaves as expected', async function(assert): Promise<void> {
    stubService('configurator', {
      getConfig(): boolean {
        return true;
      }
    });

    await render(hbs`<Social::Containers::SocialMetadata />`);
    assert.ok(this.element, 'Initial render is without errors');
    assert.dom(baseSelector);

    this.set('entity', datasetEntity);

    await render(hbs`<Social::Containers::SocialMetadata @entity={{this.entity}}/>`);
    await waitFor(`${baseSelector}__entry`, { timeout: 5000 });

    assert.equal(findAll(`${baseSelector}__entry`).length, 3, 'Renders 3 pieces of metadata');

    const likesCount = find(`${baseSelector}__count[name="likes-count"]`) as Element;
    const followsCount = find(`${baseSelector}__count[name="follows-count"]`) as Element;
    assert.equal((likesCount.textContent as string).trim(), '3', 'Correct number of likes displayed');
    assert.equal((followsCount.textContent as string).trim(), '1', 'Correct number of follows displayed');
  });
});
