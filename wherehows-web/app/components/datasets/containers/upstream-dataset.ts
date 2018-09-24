import Component from '@ember/component';
import { task, TaskInstance } from 'ember-concurrency';
import { get, set, computed, setProperties, getProperties } from '@ember/object';
import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import { assert } from '@ember/debug';
import { IUpstreamWithComplianceMetadata } from 'wherehows-web/typings/app/datasets/lineage';
import { datasetsWithComplianceMetadata } from 'wherehows-web/constants/datasets/lineage';
import { arraySome } from 'wherehows-web/utils/array';
import { IDatasetRetention, IGetDatasetRetentionResponse } from 'wherehows-web/typings/api/datasets/retention';
import { readDatasetRetentionByUrn, saveDatasetRetentionByUrn } from 'wherehows-web/utils/api/datasets/retention';
import { readPlatforms } from 'wherehows-web/utils/api/list/platforms';
import { getSupportedPurgePolicies, PurgePolicy } from 'wherehows-web/constants';
import { IDataPlatform } from 'wherehows-web/typings/api/list/platforms';
import { action } from '@ember-decorators/object';
import { retentionObjectFactory } from 'wherehows-web/constants/datasets/retention';
import Notifications, { NotificationEvent } from 'wherehows-web/services/notifications';
import { IComplianceInfo } from 'wherehows-web/typings/api/datasets/compliance';
import { service } from '@ember-decorators/service';
import { LineageList } from 'wherehows-web/typings/api/datasets/relationships';
import { readUpstreamDatasetsByUrn } from 'wherehows-web/utils/api/datasets/lineage';

/**
 * Aliases the yieldable values for the container task
 * @alias {IterableIterator<TaskInstance<TaskInstance<Promise<IDatasetView[]>> | Promise<IUpstreamWithComplianceMetadata[]>> | TaskInstance<Promise<IDataPlatform[]>> | TaskInstance<Promise<IGetDatasetRetentionResponse | null>>>}
 */
type ContainerYieldableResult = IterableIterator<
  | TaskInstance<TaskInstance<Promise<LineageList>> | Promise<IUpstreamWithComplianceMetadata[]>>
  | TaskInstance<Promise<IDataPlatform[]>>
  | TaskInstance<Promise<IGetDatasetRetentionResponse | null>>
>;

export default class UpstreamDatasetContainer extends Component {
  /**
   * References the application notifications service
   * @memberof UpstreamDatasetContainer
   * @type {ComputedProperty<Notifications>}
   */
  @service
  notifications: Notifications;

  /**
   * urn for the child dataset
   * @type {string}
   * @memberof UpstreamDatasetContainer
   */
  urn: string;

  /**
   * The platform this dataset is stored on
   * @type {IDatasetView.platform}
   * @memberof UpstreamDatasetContainer
   */
  platform: IDatasetView['platform'];

  /**
   * Purge Policy for the upstream dataset, used as the default selected option for this dataset if a current
   * Purge Policy does not exist
   * @type {IComplianceInfo.complianceType}
   * @memberof UpstreamDatasetContainer
   */
  upstreamComplianceType: IComplianceInfo['complianceType'] | undefined;

  /**
   * The list of supported purge policies for the related platform
   * @type {Array<PurgePolicy>}
   * @memberof UpstreamDatasetContainer
   */
  supportedPurgePolicies: Array<PurgePolicy> = [];

  /**
   * The list of upstream datasets for the related urn, passed in from parent container
   * @type {LineageList}
   */
  upstreamLineage: LineageList;

  /**
   * List of metadata properties for upstream datasets
   * @type {Array<IUpstreamWithComplianceMetadata>}
   * @memberof UpstreamDatasetContainer
   */
  upstreamsMetadata: Array<IUpstreamWithComplianceMetadata> = [];

  /**
   * Retention policy for the dataset with set urn
   * @type {IDatasetRetention}
   * @memberof UpstreamDatasetContainer
   */
  retention: IDatasetRetention;

  constructor() {
    super(...arguments);

    assert(`A valid child urn must be provided on instantiation, got ${this.urn}`, !!this.urn);
    assert(`A valid dataset platform must be provided on instantiation, got ${this.platform}`, !!this.platform);
    this.retention = retentionObjectFactory(this.urn);
  }

  didUpdateAttrs() {
    this._super(...arguments);
    get(this, 'getContainerDataTask').perform();
  }

  didInsertElement() {
    this._super(...arguments);
    get(this, 'getContainerDataTask').perform();
  }

  /**
   * Flags if any of the upstream datasets has an incomplete compliance policy
   * @type ComputedProperty<boolean>
   * @memberof UpstreamDatasetContainer
   */
  hasIncompleteUpstream = computed('upstreamsMetadata.[]', function(this: UpstreamDatasetContainer): boolean {
    const upstreamsMetadata = get(this, 'upstreamsMetadata');
    const upstreamIsIncomplete = ({ hasCompliance }: IUpstreamWithComplianceMetadata): boolean => !hasCompliance;

    return arraySome(upstreamIsIncomplete)(upstreamsMetadata);
  });

  /**
   * Performs tasks related to container data instantiation
   * @type {Task<ContainerYieldableResult>}
   * @memberof UpstreamDatasetContainer
   */
  getContainerDataTask = task(function*(this: UpstreamDatasetContainer): ContainerYieldableResult {
    yield get(this, 'getUpstreamMetadataTask').perform();
    yield get(this, 'getPlatformPoliciesTask').perform();
    yield get(this, 'getRetentionTask').perform();
  });

  /**
   * Task to get properties for the upstream dataset
   * @type {Task<Promise<Array<IDatasetView>>>, (a?: {} | undefined) => TaskInstance<Promise<Array<IDatasetView>>>>}
   * @memberof UpstreamDatasetContainer
   */
  getUpstreamDatasetsTask = task(function*(this: UpstreamDatasetContainer): IterableIterator<Promise<LineageList>> {
    const upstreamDatasets: LineageList = yield readUpstreamDatasetsByUrn(get(this, 'urn'));
    return set(this, 'upstreamLineage', upstreamDatasets);
  });

  /**
   * Task to get and set upstream metadata for upstream datasets
   * @type {Task<TaskInstance<Promise<Array<IDatasetView>>> | Promise<Array<IUpstreamWithComplianceMetadata>>>}
   * @memberof UpstreamDatasetContainer
   */
  getUpstreamMetadataTask = task(function*(
    this: UpstreamDatasetContainer
  ): IterableIterator<TaskInstance<Promise<LineageList>> | Promise<Array<IUpstreamWithComplianceMetadata>>> {
    // Fallback logic, if we have lineage then use that otherwise get upstreams again
    const upstreamLineage =
      get(this, 'upstreamLineage') || <LineageList>yield get(this, 'getUpstreamDatasetsTask').perform();
    const upstreamMetadataPromises = datasetsWithComplianceMetadata(upstreamLineage.map(lineage => lineage.dataset));
    const upstreamsMetadata: Array<IUpstreamWithComplianceMetadata> = yield Promise.all(upstreamMetadataPromises);

    set(this, 'upstreamsMetadata', upstreamsMetadata);
  }).restartable();

  /**
   * Task to retrieve platform policies and set supported policies for the current platform
   * @type {Task<Promise<Array<IDataPlatform>>, () => TaskInstance<Promise<Array<IDataPlatform>>>>}
   * @memberof UpstreamDatasetContainer
   */
  getPlatformPoliciesTask = task(function*(
    this: UpstreamDatasetContainer
  ): IterableIterator<Promise<Array<IDataPlatform>>> {
    const platform = get(this, 'platform');

    if (platform) {
      set(this, 'supportedPurgePolicies', getSupportedPurgePolicies(platform, yield readPlatforms()));
    }
  }).restartable();

  /**
   * Task to get the retention policy for the related child dataset
   * @type {Task<Promise<IGetDatasetRetentionResponse | null>, () => TaskInstance<Promise<IGetDatasetRetentionResponse | null>>>}
   * @memberof UpstreamDatasetContainer
   */
  getRetentionTask = task(function*(
    this: UpstreamDatasetContainer
  ): IterableIterator<Promise<IGetDatasetRetentionResponse | null>> {
    const retentionResponse: IGetDatasetRetentionResponse | null = yield readDatasetRetentionByUrn(get(this, 'urn'));
    const retention =
      retentionResponse !== null ? retentionResponse.retentionPolicy : retentionObjectFactory(get(this, 'urn'));

    setProperties(get(this, 'retention'), retention);
  }).restartable();

  /**
   * Task to update the dataset's retention policy with the user entered changes
   * @type {Task<Promise<IDatasetRetention>, () => TaskInstance<Promise<IDatasetRetention>>>}
   * @memberof UpstreamDatasetContainer
   */
  saveRetentionTask = task(function*(this: UpstreamDatasetContainer): IterableIterator<Promise<IDatasetRetention>> {
    const {
      retention,
      notifications: { notify }
    } = getProperties(this, ['retention', 'notifications']);

    setProperties(retention, yield saveDatasetRetentionByUrn(get(this, 'urn'), retention));

    notify(NotificationEvent.success, {
      content: 'Successfully updated retention policy for dataset'
    });
  }).drop();

  /**
   * Handles user action to change the dataset retention policy purgeType
   * @param {PurgePolicy} purgePolicy
   * @memberof UpstreamDatasetContainer
   */
  @action
  onRetentionPolicyChange(purgePolicy: PurgePolicy) {
    set(get(this, 'retention'), 'purgeType', purgePolicy);
  }
}
