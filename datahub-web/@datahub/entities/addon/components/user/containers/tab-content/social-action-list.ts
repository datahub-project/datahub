import Component from '@glimmer/component';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { IEntityRenderCommonPropsSearch } from '@datahub/data-models/types/search/search-entity-render-prop';
import { SocialAction } from '@datahub/data-models/constants/entity/person/social-actions';
import { PersonTab } from '@datahub/data-models/constants/entity/person/tabs';
import { tracked } from '@glimmer/tracking';
import { action } from '@ember/object';
import { DataModelEntity } from '@datahub/data-models/constants/entity';

interface IUserSocialActionListTabContentArgs {
  /**
   * Entity that represents the context of who we are showing this tab for
   */
  entity?: PersonEntity;
  /**
   * Given parameters from the profile page content parent component. With the selected tab, we can
   * reason about the type of list for which to display social action information to our person
   * entity context
   */
  params?: { selectedTab: string };
}

/**
 * This component is used in the "Liked Entities" and "Followed Entities" tab on the user entity
 * page. Its purpose is to utilize the necessary information on "actionable entities", which are
 * entities that can be targets of the social action this component is currently being used for,
 * and make a call to get all of the entities that have been targets of that social action by the
 * current context user entity.
 */
export default class UserSocialActionListTabContent extends Component<IUserSocialActionListTabContentArgs> {
  /**
   * Current page handling for search part of the social action list
   */
  // TODO: [META-11283] Add current page to query params for this and all ownership entities on
  // entity page
  @tracked
  currentPage = 1;

  /**
   * Depending on the social action for which we are using this component, the keyword we need to
   * filter for that action will be different
   */
  actionTypeToFilterKeywordMap: Partial<Record<SocialAction, string>> = {
    [SocialAction.LIKE]: 'likedBy',
    [SocialAction.FOLLOW]: 'followedBy'
  };

  /**
   * Current social action for this container, decided by the map of tab to social action
   */
  get currentSocialAction(): SocialAction {
    const { params } = this.args;
    const mapSelectedTabToSocialAction = {
      [`${PersonTab.UserSocialActionList}-like`]: SocialAction.LIKE,
      [`${PersonTab.UserSocialActionList}-follow`]: SocialAction.FOLLOW
    };

    return params?.selectedTab ? mapSelectedTabToSocialAction[params.selectedTab] : SocialAction.LIKE;
  }

  /**
   * The keyword that determines the filter token for the keyword in order to actually retrieve the
   * list of entities that match the social action performed by the specified user
   */
  get filterByKeyword(): string {
    const { currentSocialAction, actionTypeToFilterKeywordMap } = this;
    return (actionTypeToFilterKeywordMap[currentSocialAction] ||
      actionTypeToFilterKeywordMap[SocialAction.LIKE]) as string;
  }

  /**
   * The currently actionable entities for social actions that we can search through to find the
   * entities that a person has performed the social action on
   */
  get socialActionableEntities(): Array<DataModelEntity> {
    // TODO: [meta-11281] Add all applicable entities
    // Since datasets is the only likeable/followable entity right now, we don't worry about
    // having to figure out all the things that are likable/followable yet
    return [DatasetEntity];
  }

  /**
   * Since we currently only have one actionable entity, we aren't worried about passing the
   * entirety of socialActionableEntities to search config. This is a convenience to only worry
   * about datasets as an entity
   * TODO: [META-11281] Add all entities
   */
  get currentActionableEntity(): DataModelEntity {
    return this.socialActionableEntities[0];
  }

  /**
   * The search config should vary by whichever social actionable entity is used to perform the
   * actual search piece
   */
  get searchConfig(): IEntityRenderCommonPropsSearch | void {
    // TODO: [META-11281] Add other entities
    // Right now only datasets are likeable/followable so we do no worry about constructing the
    // configs for other social actionable entities
    return DatasetEntity.renderProps.search;
  }

  /**
   * Switches to a new page per our dependency on the search for our entity list
   * @param newPage - new page number to change to
   */
  @action
  onUpdatePage(newPage: number): void {
    this.currentPage = newPage;
  }
}
