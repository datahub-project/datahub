import Component from '@ember/component';
import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import { computed, action } from '@ember/object';
import { or } from '@ember/object/computed';
import { get } from '@ember/object';
import { set } from '@ember/object';
import { setProperties } from '@ember/object';
import { next } from '@ember/runloop';
import { getConfig } from 'wherehows-web/services/configurator';
import { IAppConfig } from '@datahub/shared/types/configurator/configurator';
import { inject as service } from '@ember/service';
import RouterService from '@ember/routing/router-service';
import { classNames } from '@ember-decorators/component';
import { Tab, TabProperties } from '@datahub/data-models/constants/entity/shared/tabs';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';
import { DatasetEntity, createDatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { ISearchEntityRenderProps } from '@datahub/data-models/types/entity/rendering/search-entity-render-prop';
import { encodeUrn } from '@datahub/utils/validators/urn';
import { IDatasetApiView } from '@datahub/metadata-types/types/entity/dataset/dataset-entity';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { task } from 'ember-concurrency';
import { ETaskPromise } from '@datahub/utils/types/concurrency';

/**
 * This file is a replacement of the DatasetController. Still there is too much
 * going on here. It would be nice in the future to extract some parts out.
 */
@classNames('dataset-main')
@containerDataSource('containerDataTask', ['dataset'])
export default class DatasetMainContainer extends Component {
  /**
   * Inject the router as there are some transitions that we need to do
   * @type {Route}
   * @memberof DatasetMainContainer
   */
  @service
  router: RouterService;

  /**
   * References the dataset
   * @type {IDatasetView}
   * @memberof DatasetMainContainer
   */
  dataset: IDatasetView;

  /**
   * Current metricUrn query parameter
   * @type {string}
   * @memberof DatasetMainContainer
   */
  metricUrn: string;

  /**
   * URN for the current dataset view
   * @type {(string | void)}
   * @memberof DatasetMainContainer
   */
  urn: string;

  /**
   * Enum of tab properties
   * @type {Tab}
   * @memberof DatasetMainContainer
   */
  tabIds = Tab;

  /**
   * Enum of tabs components to render each tab content
   * @type {TabProperties}
   * @memberof DatasetMainContainer
   */
  tabsComponent = TabProperties;

  /**
   * The currently selected tab in view
   * @type {Tabs}
   * @memberof DatasetMainContainer
   */
  tabSelected: Tab;

  /**
   * Flag indicating if the compliance info does not have user entered information
   * @type {boolean}
   * @memberof DatasetMainContainer
   */
  isNewComplianceInfo: boolean;

  /**
   * Flag indicating there are fields in the compliance policy that have not been updated by a user
   * @type {boolean}
   * @memberof DatasetMainContainer
   */
  compliancePolicyHasDrift: boolean;

  /**
   * References the collection of help links with references to external pages of help information
   * @type {IAppConfig.wikiLinks}
   */
  wikiLinks: IAppConfig['wikiLinks'] = getConfig('wikiLinks') || {};

  /**
   * References a collection of properties for avatar properties
   * @type {IAppConfig.avatarEntityProps}
   */
  avatarEntityProps: IAppConfig['userEntityProps'] = getConfig('userEntityProps');

  /**
   * Flag indicating the dataset policy is derived from an upstream source
   * @type {boolean}
   * @memberof DatasetMainContainer
   */
  isPolicyFromUpstream = false;

  /**
   * Flag indicating if the viewer is internal
   * @type {boolean}
   * @memberof DatasetMainContainer
   */
  isInternal: boolean = Boolean(getConfig('isInternal'));

  /**
   * Flag indicating whether or not we are in the staging environment, which is useful for determining whether
   * or not to display some features in testing
   * @type {boolean}
   * @memberof DatasetMainContainer
   */
  isStaging: boolean = getConfig('isStagingBanner') || false;

  /**
   * Flags the lineage feature for datasets
   * @type {boolean}
   * @memberof DatasetMainContainer
   */
  shouldShowDatasetLineage: boolean = getConfig('shouldShowDatasetLineage');

  /**
   * Flags the institutional memory feature for entities, in this case datasets
   * @memberof DatasetMainContainer
   */
  shouldShowInstitutionalMemory: boolean = getConfig('showInstitutionalMemory');

  /**
   * Whether or not we have met the dataset ownership count requirements
   * @type {boolean}
   * @memberof DatasetMainContainer
   */
  datasetOwnersRequiredNotMet: boolean;

  /**
   * Flag indicating that the dataset ownership requires user attention
   * @type {ComputedProperty<boolean>}
   */
  @or('datasetOwnersRequiredNotMet')
  ownershipRequiresUserAction: boolean;

  /**
   * Flag indicating that the compliance policy needs user attention
   * @type {ComputedProperty<boolean>}
   */

  @or('isNewComplianceInfo', 'compliancePolicyHasDrift')
  requiresUserAction: boolean;

  /**
   * Flag to say if this dataset is a pinot dataset
   * @type {boolean}
   * @memberof DatasetMainContainer
   */
  isPinot: boolean;

  /**
   * Stores the entity definition for a dataset to be passed into the template
   */
  fields: Array<ISearchEntityRenderProps> = DatasetEntity.renderProps.search.attributes;

  /**
   * This value will only be read by non-UMP dataset header, and powers whether
   * the tag will show that we have PII in the dataset or not
   */
  datasetContainsPersonalData: boolean;

  /**
   * Reference to the DatasetEntity
   * @type {DatasetEntity}
   */
  entity?: DatasetEntity;

  /**
   * List of tasks to perform on instantiation of this container
   */
  @(task(function*(this: DatasetMainContainer): IterableIterator<Promise<DatasetEntity | boolean>> {
    yield this.reifyEntityTask.perform();
  }).restartable())
  containerDataTask!: ETaskPromise<DatasetEntity | boolean>;

  /**
   * Materializes the DatasetEntity instance
   */
  @(task(function*(this: DatasetMainContainer): IterableIterator<Promise<DatasetEntity>> {
    // TODO should fetch the dataset itself, but right now dataset is fetched at route level
    const { dataset } = this;

    if (dataset) {
      const entity: DatasetEntity = yield createDatasetEntity(this.dataset.uri, this.dataset as IDatasetApiView);

      set(this, 'entity', entity);
    }
  }).restartable())
  reifyEntityTask!: ETaskPromise<DatasetEntity>;

  /**
   * This will return the paths for an entity. We should be able to consume entity.readPath
   * direcly but since datasets are not migrated we need to flag guard it.
   *
   * TODO META-8863 Interim implementation to read categories for datasets
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
   * Converts the uri on a model to a usable URN format
   * @type {ComputedProperty<string>}
   */
  @computed('dataset.uri')
  get encodedUrn(): string {
    const { uri } = this.dataset || { uri: '' };
    return encodeUrn(uri);
  }

  /**
   * Array of tabs that are available for this entity
   * @type {Array<Tab>}
   */
  @computed('shouldShowDatasetLineage', 'isPinot', 'dataset.platform')
  get datasetTabs(): Array<Tab> {
    const { isPinot, shouldShowDatasetLineage, dataset } = this;
    if (!dataset) {
      return [];
    }
    const tabs: Array<Tab> = [];

    tabs.push(Tab.Schema);
    if (!isPinot) {
      tabs.push(Tab.Properties);
    }
    tabs.push(Tab.Ownership);

    if (shouldShowDatasetLineage) {
      tabs.push(Tab.Relationships);
    }

    tabs.push(Tab.Wiki);

    return tabs;
  }

  /**
   * Map to say which tab we should highlight with a notification badge
   * @type {{ [key in Tabs]?: boolean }}
   * @memberof DatasetMainContainer
   */
  @computed('requiresUserAction', 'ownershipRequiresUserAction')
  get notificationsMap(): { [key in Tab]?: boolean } {
    // TODO: FIX this comment and remove
    // somehow computed properties does not work well with this.XXX;
    return {
      [Tab.Ownership]: get(this, 'ownershipRequiresUserAction')
    };
  }

  /**
   * Will return the current component for the header of the entity. Right now
   * we have two headers: regular and ump.
   * @type {string}
   * @memberof DatasetMainContainer
   */
  @computed('dataset.platform')
  get headerComponent(): string {
    const { dataset } = this;
    if (!dataset) {
      return '';
    }
    const { platform } = dataset;
    if (platform === DatasetPlatform.UMP) {
      return 'datasets/dataset-ump-header';
    }
    return 'datasets/dataset-header';
  }

  /**
   * Handles user generated tab selection action by transitioning to specified route
   * @param {Tabs} tabSelected the currently selected tab
   * @returns {void}
   * @memberof DatasetMainContainer
   */
  @action
  tabSelectionChanged(tabSelected: Tab): void {
    // if the tab selection is not same as current, transition
    if (this.tabSelected !== tabSelected) {
      const router = this.router;
      router.transitionTo(router.currentRouteName, this.encodedUrn, tabSelected, {
        queryParams: {
          metric_urn: this.metricUrn // eslint-disable-line @typescript-eslint/camelcase
        }
      });
    }
  }

  /**
   * Updates the isNewComplianceInfo flag if the policy is not from an upstream dataset, otherwise set to false
   * Also sets the isPolicyFromUpstream attribute
   * @param {({
   *     isNewComplianceInfo: boolean;
   *     fromUpstream: boolean;
   *   })} {
   *     isNewComplianceInfo,
   *     fromUpstream
   *   }
   * @memberof DatasetMainContainer
   */
  @action
  setOnComplianceTypeChange({
    isNewComplianceInfo,
    fromUpstream
  }: {
    isNewComplianceInfo: boolean;
    fromUpstream: boolean;
  }): void {
    setProperties(this, {
      isNewComplianceInfo: !fromUpstream && isNewComplianceInfo,
      isPolicyFromUpstream: fromUpstream
    });

    if (fromUpstream) {
      this.setOnChangeSetDrift(false);
    }
  }

  /**
   * Setter to update the drift flag
   * @param {boolean} hasDrift
   * @memberof DatasetMainContainer
   */
  @action
  setOnChangeSetDrift(hasDrift: boolean): void {
    set(this, 'compliancePolicyHasDrift', !this.isPolicyFromUpstream && hasDrift);
  }

  /**
   * Triggered when the ownership information changes, will alert the user on the tab with a red dot if
   * the current state of the dataset doesn't match the rules set out for the dataset ownership
   * @param ownersNotConfirmed - Whether or not the owners for the dataset meet the requirements
   */
  @action
  setOwnershipRuleChange(ownersNotConfirmed: boolean): void {
    next(this, (): boolean => set(this, 'datasetOwnersRequiredNotMet', ownersNotConfirmed));
  }
}
