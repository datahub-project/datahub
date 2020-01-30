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
import { IEntityRenderProps } from '@datahub/data-models/types/entity/rendering/entity-render-props';
import { ITabProperties } from '@datahub/data-models/constants/entity/shared/tabs';
import { set, computed, action } from '@ember/object';
import { containerDataSource } from '@datahub/utils/api/data-source';
import CurrentUser from '@datahub/shared/services/current-user';
import RouterService from '@ember/routing/router-service';
import { PersonTab, getPersonTabPropertiesFor } from '@datahub/data-models/constants/entity/person/tabs';
import { generateTabId } from '@datahub/user/utils/tabownership';
import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { capitalize } from '@ember/string';
import { humanize } from 'ember-cli-string-helpers/helpers/humanize';

@layout(template)
@tagName('')
@containerDataSource<UserMainContainer>('getContainerDataTask', ['personUrn'])
export default class UserMainContainer extends Component {
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
      set(this, 'renderProps', this.appendOwnershipTab(PersonEntityClass.allRenderProps));
    }
  }).restartable())
  getContainerDataTask!: ETaskPromise<void>;

  /**
   * Will append the dynamically generated ownership tabs to the the renderProps
   * @param allRenderProps render props for person
   */
  appendOwnershipTab(allRenderProps: typeof PersonEntity['allRenderProps']): typeof PersonEntity['allRenderProps'] {
    const { dataModels } = this;
    const unGuardedEntities = dataModels.guards.unGuardedEntities;
    const ownershipEntities = unGuardedEntities
      .filter((entity: DataModelEntity): boolean => entity.displayName !== PersonEntity.displayName)
      .map(
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
          [PersonTab.UserOwnership]: [...ownershipEntities, ...getPersonTabPropertiesFor([PersonTab.UserUMPFlows])]
        }
      }
    };
  }

  /**
   * Flags whether the currently logged in user is the same as the person whose entity is the
   * context given to this container
   */
  @computed('entity', 'currentUser.currentUser')
  get isCurrentUser(): boolean {
    const { currentUser, entity } = this;
    const loggedInUser = currentUser.currentUser || null;
    const contextUser = (entity && entity.username) || '';

    return !!loggedInUser && loggedInUser.userName === contextUser;
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
