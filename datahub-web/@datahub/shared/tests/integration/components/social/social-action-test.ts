import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { baseSocialActionComponentClass } from '@datahub/shared/components/social/social-action';
import { SocialAction } from '@datahub/data-models/constants/entity/person/social-actions';
import { noop } from 'lodash';
import { stubService } from '@datahub/utils/test-helpers/stub-service';
import { setAspect } from '@datahub/data-models/entity/utils/aspects';

module('Integration | Component | social/social-action', function(hooks): void {
  setupRenderingTest(hooks);

  const baseSelector = `.${baseSocialActionComponentClass}`;

  const personEntity = new PersonEntity('aketchum');
  const sampleEntity = new DatasetEntity('pikachu');

  // Modifies the addLike function to work with our test instance more easily
  sampleEntity.addLike = function(): Promise<void> {
    const likedByUrns = sampleEntity.likedByUrns;
    const setOfLikedByUrns = new Set(likedByUrns);
    setOfLikedByUrns.add(personEntity.urn);

    setAspect(sampleEntity, 'likes', {
      actions: Array.from(setOfLikedByUrns).map((urn): Com.Linkedin.Common.LikeAction => ({ likedBy: urn }))
    });
    return new Promise(noop);
  };

  // Modifies the removeLike function to work with our test instance more easily
  sampleEntity.removeLike = function(): Promise<void> {
    const likedByUrns = sampleEntity.likedByUrns;
    const setOfLikedByUrns = new Set(likedByUrns);
    setOfLikedByUrns.delete(personEntity.urn);

    setAspect(sampleEntity, 'likes', {
      actions: Array.from(setOfLikedByUrns).map((urn): Com.Linkedin.Common.LikeAction => ({ likedBy: urn }))
    });
    return new Promise(noop);
  };

  sampleEntity.addFollow = function(): Promise<void> {
    const followedByUrns = sampleEntity.followedByUrns;
    const setOfFollowedByUrns = new Set(followedByUrns);
    setOfFollowedByUrns.add(personEntity.urn);

    setAspect(sampleEntity, 'follow', {
      followers: Array.from(setOfFollowedByUrns).map(
        (urn): Com.Linkedin.Common.FollowAction => ({ follower: { corpUser: urn } })
      )
    });
    return new Promise(noop);
  };

  test('it works and modifies current entity as expected', async function(assert): Promise<void> {
    stubService('current-user', { entity: personEntity });
    stubService('configurator', {
      getConfig(): boolean {
        return true;
      }
    });

    this.setProperties({
      entity: sampleEntity,
      type: SocialAction.LIKE
    });

    await render(hbs`<Social::SocialAction
                       @entity={{this.entity}}
                       @type={{this.type}}
                     />`);

    assert.ok(this.element, 'Initial render is without errors');
    assert.dom(baseSelector);
    assert.equal(
      findAll(`${baseSelector}__icon--active`).length,
      0,
      'Our button is not considered "active" yet since the entity is not in the user liked urns'
    );

    await click(baseSelector);

    assert.equal(
      sampleEntity.likedByUrns.length,
      1,
      'Sanity check: making sure current user entity was modified by clicking the button'
    );

    assert.dom(`${baseSelector}__icon--active`);

    await click(baseSelector);

    assert.equal(
      sampleEntity.likedByUrns.length,
      0,
      'Making sure that the correct function is called when clicking an active icon'
    );

    this.set('type', SocialAction.FOLLOW);

    await render(hbs`<Social::SocialAction
                       @entity={{this.entity}}
                       @type={{this.type}}
                     />`);

    assert.equal(
      findAll(`${baseSelector}__icon--active`).length,
      0,
      'Our button is not considered "active" yet for follows'
    );

    await click(baseSelector);

    assert.equal(
      sampleEntity.followedByUrns.length,
      1,
      'Sanity check: making sure current user entity was modified by clicking the button'
    );
  });
});
