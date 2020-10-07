import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/datasets/containers/dataset-main';
import { computed, action } from '@ember/object';
import { or } from '@ember/object/computed';
import { set } from '@ember/object';
import { setProperties } from '@ember/object';
import { next } from '@ember/runloop';
import { inject as service } from '@ember/service';
import RouterService from '@ember/routing/router-service';
import { layout, classNames } from '@ember-decorators/component';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { task } from 'ember-concurrency';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import DataModelsService from '@datahub/data-models/services/data-models';
import { isNotFoundApiError } from '@datahub/utils/api/shared';
import Search from '@datahub/shared/services/search';
import { ISearchEntityRenderProps } from '@datahub/data-models/types/search/search-entity-render-prop';
import { IAppConfig, IConfigurator } from '@datahub/shared/types/configurator/configurator';
import { DatasetTab } from '@datahub/data-models/constants/entity/dataset/tabs';
import { CommonTabProperties } from '@datahub/data-models/constants/entity/shared/tabs';
import { ITabProperties } from '@datahub/data-models/types/entity/rendering/entity-render-props';
import { TabProperties } from '@datahub/data-models/constants/entity/dataset/tab-properties/all';

/**
 * Defines the error properties when there is an error in the container
 */
interface IContainerError {
  isNotFoundApiError: boolean;
  error: Error;
}

/**
 * This file is a replacement of the DatasetController. Still there is too much
 * going on here. It would be nice in the future to extract some parts out.
 */
@classNames('dataset-main')
@layout(template)
@containerDataSource<DatasetMainContainer>('containerDataTask', ['urn'])
export default class DatasetMainContainer extends Component {
  /**
   * Inject the router as there are some transitions that we need to do
   */
  @service
  router!: RouterService;

  /**
   * Data Models service to load entities
   */
  @service('data-models')
  dataModels!: DataModelsService;

  /**
   * Search service to set entity type when visiting dataset page
   */
  @service('search')
  search!: Search;

  /**
   * URN for the current dataset view
   */
  urn!: string;

  /**
   * Enum of tab properties
   */
  tabIds = DatasetTab;

  /**
   * The currently selected tab in view
   */
  tabSelected!: DatasetTab;

  /**
   * Configurator service if available
   */
  @service
  configurator?: IConfigurator;

  /**
   * References the collection of help links with references to external pages of help information
   */
  wikiLinks: IAppConfig['wikiLinks'] = this.configurator?.getConfig('wikiLinks') || {};

  /**
   * References a collection of properties for avatar properties
   */
  avatarEntityProps?: IAppConfig['userEntityProps'] = this.configurator?.getConfig('userEntityProps');

  /**
   * Jit ACL configs
   */
  jitAclConfig = this.configurator?.getConfig('jitAcl');

  /**
   * Flag indicating the dataset policy is derived from an upstream source
   */
  isPolicyFromUpstream = false;

  /**
   * Flag indicating if the viewer is internal
   */
  get isInternal(): boolean {
    return Boolean(this.configurator?.getConfig('isInternal'));
  }

  /**
   * Flag indicating whether or not we are in the staging environment, which is useful for determining whether
   * or not to display some features in testing
   */
  get isStaging(): boolean {
    return Boolean(this.configurator?.getConfig('isStagingBanner'));
  }

  /**
   * Whether or not we have met the dataset ownership count requirements
   */
  datasetOwnersRequiredNotMet?: boolean;

  /**
   * Flag indicating that the dataset ownership requires user attention
   * @type {ComputedProperty<boolean>}
   */
  @or('datasetOwnersRequiredNotMet')
  ownershipRequiresUserAction!: boolean;

  /**
   * Stores the entity definition for a dataset to be passed into the template
   */
  fields: Array<ISearchEntityRenderProps> = DatasetEntity.renderProps.search.attributes;

  /**
   * This value will only be read by non-UMP dataset header, and powers whether
   * the tag will show that we have PII in the dataset or not
   */
  datasetContainsPersonalData?: boolean;

  /**
   * Reference to the DatasetEntity
   */
  entity?: DatasetEntity;

  /**
   * Reference to the entity class for use by downstream components, for example, to access the Entity's render props
   */
  get entityClass(): typeof DatasetEntity {
    return this.dataModels.getModel(DatasetEntity.displayName);
  }

  /**
   * Indicate if the container is in a error state and what error ocurred
   */
  error?: IContainerError;

  /**
   * List of tasks to perform on instantiation of this container
   */
  @(task(function*(this: DatasetMainContainer): IterableIterator<Promise<DatasetEntity | boolean>> {
    this.resetState();

    try {
      yield this.reifyEntityTask.perform();

      yield this.getPiiStatusTask.perform();
    } catch (e) {
      set(this, 'error', {
        isNotFoundApiError: isNotFoundApiError(e),
        error: e
      });
    }
  }).restartable())
  containerDataTask!: ETaskPromise<DatasetEntity | boolean>;

  /**
   * Materializes the DatasetEntity instance
   */
  @(task(function*(this: DatasetMainContainer): IterableIterator<Promise<DatasetEntity>> {
    const { dataModels, urn } = this;

    //TODO: META-8267 Container notifications decorator
    if (urn) {
      const entity: DatasetEntity | undefined = yield dataModels.createInstance(DatasetEntity.displayName, urn);
      set(this, 'entity', entity);
    }
  }).restartable())
  reifyEntityTask!: ETaskPromise<DatasetEntity>;

  /**
   * Gets the PII status for any non UMP dataset
   */
  @(task(function*(this: DatasetMainContainer): IterableIterator<Promise<boolean>> {
    const { entity } = this;
    if (entity) {
      const piiStatus = yield entity.readPiiStatus();
      set(this, 'datasetContainsPersonalData', piiStatus);
    }
  }).restartable())
  getPiiStatusTask!: ETaskPromise<boolean>;

  /**
   * Resets the state of the container as it may be reused for different datasets
   */
  resetState(): void {
    setProperties(this, {
      error: undefined,
      datasetOwnersRequiredNotMet: undefined,
      datasetContainsPersonalData: undefined,
      entity: undefined,
      isPolicyFromUpstream: false
    });
  }

  /**
   * This will return the paths for an entity. We should be able to consume entity.readPath
   * direcly but since datasets are not migrated we need to flag guard it.
   *
   */
  @computed('entity')
  get paths(): Promise<Array<string>> {
    const { entity } = this;
    if (!entity) {
      return Promise.resolve([]);
    }

    return entity.readPath;
  }
  /**
   * Array of tabs that are available for this entity. Is a computed property as there is a possibility the dataset
   * entity is initially undefined and will be applied following first render
   */
  @computed('entity')
  get datasetTabs(): Array<ITabProperties> {
    const { entity } = this;
    if (!entity) {
      return [];
    }
    const tabs: Array<string> = [
      DatasetTab.Schema,
      DatasetTab.Properties,
      DatasetTab.Status,
      DatasetTab.Ownership,
      DatasetTab.Relationships
    ];

    return [...TabProperties.filter((tab): boolean => tabs.includes(tab.id)), ...CommonTabProperties];
  }

  /**
   * Map to say which tab we should highlight with a notification badge
   */
  @computed('requiresUserAction', 'ownershipRequiresUserAction')
  get notificationsMap(): { [key in DatasetTab]?: boolean } {
    return {
      [DatasetTab.Ownership]: this.ownershipRequiresUserAction
    };
  }

  /**
   * Handler to capture changes in dataset PII status
   */
  @action
  onNotifyPiiStatus(containingPersonalData: boolean): void {
    set(this, 'datasetContainsPersonalData', containingPersonalData);
  }

  /**
   * Handles user generated tab selection action by transitioning to specified route
   * @param {Tabs} tabSelected the currently selected tab
   */
  @action
  tabSelectionChanged(tabSelected: DatasetTab): void {
    // if the tab selection is not same as current, transition
    if (this.tabSelected !== tabSelected) {
      const router = this.router;
      router.transitionTo(router.currentRouteName, this.urn, tabSelected);
    }
  }

  /**
   * Triggered when the ownership information changes, will alert the user on the tab with a red dot if
   * the current state of the dataset doesn't match the rules set out for the dataset ownership
   * @param ownersNotConfirmed - Whether or not the owners for the dataset meet the requirements
   */
  @action
  setOwnershipRuleChange(ownersNotConfirmed: boolean): void {
    next(this, (): boolean | undefined => set(this, 'datasetOwnersRequiredNotMet', ownersNotConfirmed));
  }
}
