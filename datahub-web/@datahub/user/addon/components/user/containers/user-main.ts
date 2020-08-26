import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/user/containers/user-main';
import { layout, tagName } from '@ember-decorators/component';
import { task } from 'ember-concurrency';
import { inject as service } from '@ember/service';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import DataModelsService from '@datahub/data-models/services/data-models';
import { IPersonEntitySpecificConfigs } from '@datahub/data-models/entity/person/render-props';
import { IEntityRenderProps, ITabProperties } from '@datahub/data-models/types/entity/rendering/entity-render-props';
import { set, computed, action } from '@ember/object';
import { containerDataSource } from '@datahub/utils/api/data-source';
import CurrentUser from '@datahub/shared/services/current-user';
import RouterService from '@ember/routing/router-service';
import { PersonTab, getPersonTabPropertiesFor } from '@datahub/data-models/constants/entity/person/tabs';
import { generateTabId } from '@datahub/user/utils/tabownership';
import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { capitalize } from '@ember/string';
import { humanize } from 'ember-cli-string-helpers/helpers/humanize';
import { IConfigurator } from '@datahub/shared/types/configurator/configurator';
import { SocialAction } from '@datahub/data-models/constants/entity/person/social-actions';
import { pastTense } from '@datahub/utils/helpers/past-tense';
import { pendingSocialActions } from '@datahub/shared/constants/social/pending-actions';
import { isOwnableEntity } from '@datahub/data-models/utils/ownership';

@layout(template)
@tagName('')
@containerDataSource<UserMainContainer>('getContainerDataTask', ['personUrn'])
export default class UserMainContainer extends Component {
  /**
   * Injection of the configurator service to read relevant user and social action configs
   */
  @service
  configurator!: IConfigurator;

  /**
   * Injection of data modeling service to get the current implementation of the user's person
   * entity.
   */
  @service('data-models')
  dataModels!: DataModelsService;

  /**
   * Injects the current user service to help us inform the rest of our children whether or not the
   * person whose context fills this component is the same as the current user logged into our
   * application
   */
  @service('current-user')
  currentUser!: CurrentUser;

  /**
   * Inject the router as there are some transitions that we need to do
   */
  @service
  router!: RouterService;

  /**
   * A given user id or urn from the route's query params
   */
  personUrn?: string;

  /**
   * The currently active tab under the user main profile page
   */
  tabSelected?: string;

  /**
   * Assigned by the data fetching task, is the context of the person entity given to all contained
   * components
   */
  entity?: PersonEntity;

  /**
   * Rendering properties to tell our contained components how to determine what to show and other
   * configurable behaviors
   */
  renderProps?: IPersonEntitySpecificConfigs & IEntityRenderProps;

  /**
   * Calls the person entity model to get the data necessary to populate the entity header
   * component on the person entity profile page
   */
  @(task(function*(this: UserMainContainer): IterableIterator<Promise<PersonEntity | void>> {
    const { renderProps, personUrn, dataModels } = this;

    const PersonEntityClass = dataModels.getModel(PersonEntity.displayName);

    if (personUrn) {
      const entity = yield dataModels.createInstance(PersonEntity.displayName, personUrn);
      set(this, 'entity', entity);
    }

    if (!renderProps) {
      set(this, 'renderProps', this.userProfileTabs(PersonEntityClass.allRenderProps));
    }
  }).restartable())
  getContainerDataTask!: ETaskPromise<void>;

  /**
   * Generate the user profile tabs that are available for this entity
   * @param allRenderProps
   */
  userProfileTabs(allRenderProps: typeof PersonEntity['allRenderProps']): typeof PersonEntity['allRenderProps'] {
    return this.appendSocialTabs(this.appendOwnershipTab(allRenderProps));
  }

  /**
   * Will append the dynamically generated ownership tabs to the the renderProps
   * @param allRenderProps render props for person
   */
  appendOwnershipTab(allRenderProps: typeof PersonEntity['allRenderProps']): typeof PersonEntity['allRenderProps'] {
    const { dataModels } = this;
    const unGuardedEntities = dataModels.guards.unGuardedEntities;
    const ownershipEntities = unGuardedEntities.filter(isOwnableEntity).map(
      (entity: DataModelEntity): ITabProperties => ({
        id: generateTabId(entity),
        title: capitalize(humanize([entity.displayName])),
        contentComponent: 'user/containers/tab-content/entity-ownership',
        lazyRender: true
      })
    );

    return {
      ...allRenderProps,
      userProfilePage: {
        ...allRenderProps.userProfilePage,
        tablistMenuProperties: {
          ...allRenderProps.userProfilePage.tablistMenuProperties,
          [PersonTab.UserOwnership]: [...ownershipEntities]
        }
      }
    };
  }

  /**
   * Will dynamically append the number of social action list tabs to the render props based on the
   * available social actions (i.e. total number of actions minus those that are currently flag
   * guarded in our configs)
   * @param allRenderProps render props for person
   */
  appendSocialTabs(allRenderProps: typeof PersonEntity['allRenderProps']): typeof PersonEntity['allRenderProps'] {
    const { configurator } = this;
    const showSocialActions = configurator.getConfig('showSocialActions', { useDefault: true, default: false });
    const showPendingSocialActions = configurator.getConfig('showPendingSocialActions', {
      useDefault: true,
      default: false
    });

    if (!showSocialActions) {
      return allRenderProps;
    }

    const socialActions = [SocialAction.LIKE];

    const unguardedSocialActions = showPendingSocialActions
      ? [...socialActions, ...pendingSocialActions]
      : [...socialActions];

    const socialActionTabs = unguardedSocialActions.map(action => {
      const [baseTabProperties] = getPersonTabPropertiesFor([PersonTab.UserSocialActionList]);
      return {
        ...baseTabProperties,
        id: `${baseTabProperties.id}-${action}`,
        title: `${capitalize(pastTense([action]))} Entities`
      };
    });

    return {
      ...allRenderProps,
      userProfilePage: {
        ...allRenderProps.userProfilePage,
        tablistMenuProperties: {
          ...allRenderProps.userProfilePage.tablistMenuProperties,
          [PersonTab.UserLists]: [
            ...(allRenderProps.userProfilePage.tablistMenuProperties[PersonTab.UserLists] || []),
            ...socialActionTabs
          ]
        }
      }
    };
  }

  /**
   * Flags whether the currently logged in user is the same as the person whose entity is the
   * context given to this container
   */
  @computed('entity', 'currentUser.entity')
  get isCurrentUser(): boolean {
    const { currentUser, entity } = this;
    const loggedInUser = currentUser.entity || null;
    const contextUser = (entity && entity.username) || '';

    return !!loggedInUser && loggedInUser.username === contextUser;
  }

  /**
   * Handles user generated tab selection action by transitioning to specified route
   * @param {Tabs} tabSelected - the newly selected tab
   */
  @action
  tabSelectionChanged(tabSelected: string): void {
    // if the tab selection is not same as current, transition
    if (this.tabSelected !== tabSelected) {
      const { router, personUrn } = this;

      router.transitionTo(router.currentRouteName, personUrn || '', tabSelected);
    }
  }
}
