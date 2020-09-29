import Component from '@ember/component';
import { get, set, setProperties } from '@ember/object';
import { task } from 'ember-concurrency';
import { action } from '@ember/object';
import Notifications from '@datahub/utils/services/notifications';
import { NotificationEvent } from '@datahub/utils/constants/notifications';
import { IOwner, IOwnerResponse } from 'datahub-web/typings/api/datasets/owners';
import {
  OwnerType,
  readDatasetOwnersByUrn,
  readDatasetSuggestedOwnersByUrn,
  readDatasetOwnerTypesWithoutConsumer,
  updateDatasetOwnersByUrn
} from 'datahub-web/utils/api/datasets/owners';
import { inject as service } from '@ember/service';
import { getConfig } from '@datahub/shared/services/configurator';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { IAppConfig } from '@datahub/shared/types/configurator/configurator';

@containerDataSource<DatasetOwnershipContainer>('getContainerDataTask', ['urn'])
export default class DatasetOwnershipContainer extends Component {
  /**
   * The urn identifier for the dataset
   * @type {string}
   */
  urn!: string;

  /**
   * List of owners for the dataset
   * @type {Array<IOwner>}
   */
  owners: Array<IOwner> = [];

  /**
   * List of suggested owners for the dataset
   * @type {Array<IOwner>}
   */
  suggestedOwners: Array<IOwner> = [];

  /**
   * List of types available for a dataset owner
   * @type {Array<OwnerType>}
   */
  ownerTypes: Array<OwnerType>;

  /**
   * Reference to the application notifications Service
   * @type {ComputedProperty<Notifications>}
   */
  @service
  notifications: Notifications;

  /**
   * Flag indicates that a ownership metadata is inherited from an upstream dataset
   * @type {boolean}
   */
  fromUpstream = false;

  /**
   * Reference to the upstream dataset
   * @type {string}
   */
  upstreamUrn: string;

  /**
   * Avatar properties used to generate avatar images
   * @type {(IAppConfig['userEntityProps'] | undefined)}
   * @memberof DatasetOwnershipContainer
   */
  avatarProperties: IAppConfig['userEntityProps'] | undefined;

  /**
   * Metadata related to the ownership properties for the dataset
   * @type {{ actor: string, lastModified: number }}
   * @memberof DatasetOwnershipContainer
   */
  ownershipMetadata: { actor: string; lastModified: number } = { actor: '', lastModified: 0 };
  /**
   * An async parent task to group all data tasks for this container component
   */
  @task(function*(this: DatasetOwnershipContainer): IterableIterator<Promise<unknown> | unknown> {
    const { getDatasetOwnersTask, getSuggestedOwnersTask, getDatasetOwnerTypesTask, getAvatarProperties } = this;
    const tasks = [getDatasetOwnersTask, getSuggestedOwnersTask, getDatasetOwnerTypesTask, getAvatarProperties];
    yield* tasks.map((task): Promise<unknown> | unknown => task.perform());
  })
  getContainerDataTask!: ETaskPromise<unknown>;
  /**
   * Fetches & sets avatar props to build owner avatar images
   * @memberof DatasetOwnershipContainer
   */
  @task(function*(this: DatasetOwnershipContainer): IterableIterator<IAppConfig['userEntityProps']> {
    return set(this, 'avatarProperties', getConfig('userEntityProps'));
  })
  getAvatarProperties!: ETaskPromise<IAppConfig['userEntityProps']>;
  /**
   * Reads the owners for this dataset
   */
  @task(function*(this: DatasetOwnershipContainer): IterableIterator<Promise<IOwnerResponse>> {
    const { owners = [], fromUpstream, datasetUrn, lastModified, actor } = ((yield readDatasetOwnersByUrn(
      this.urn
    )) as unknown) as IOwnerResponse;

    setProperties(this, { owners, fromUpstream, upstreamUrn: datasetUrn, ownershipMetadata: { lastModified, actor } });
  })
  getDatasetOwnersTask!: ETaskPromise<IOwnerResponse>;
  /**
   * Fetches the suggested owners for this dataset
   */
  @task(function*(this: DatasetOwnershipContainer): IterableIterator<Promise<IOwnerResponse>> {
    let suggestedOwners: DatasetOwnershipContainer['suggestedOwners'] = [];

    // Ignore errors associated this requesting suggested owners
    try {
      const { owners = [] } = ((yield readDatasetSuggestedOwnersByUrn(this.urn)) as unknown) as IOwnerResponse;
      suggestedOwners = owners;
    } finally {
      setProperties(this, { suggestedOwners });
    }
  })
  getSuggestedOwnersTask!: ETaskPromise<IOwnerResponse>;
  /**
   * Reads the owner types available
   */
  @task(function*(this: DatasetOwnershipContainer): IterableIterator<Promise<Array<OwnerType>>> {
    const ownerTypes = ((yield readDatasetOwnerTypesWithoutConsumer()) as unknown) as Array<OwnerType>;
    set(this, 'ownerTypes', ownerTypes);
  })
  getDatasetOwnerTypesTask!: ETaskPromise<Array<OwnerType>>;

  /**
   * Handles user notifications when save succeeds or fails
   * @template T the return type for the save request
   * @param {Promise<T>} request to update owners
   * @returns {Promise<T>}
   * @memberof DatasetOwnershipContainer
   */
  async notifyOnSave<T>(this: DatasetOwnershipContainer, request: Promise<T>): Promise<T> {
    const { notify } = get(this, 'notifications');

    try {
      await request;
      notify({ type: NotificationEvent.success, content: 'Changes have been successfully saved!' });
    } catch (e) {
      notify({ type: NotificationEvent.error, content: 'An error occurred while saving.' });
    }

    return request;
  }

  /**
   * Persists the changes to the owners list
   * @param {Array<IOwner>} updatedOwners
   * @return {Promise<{}>}
   */
  @action
  async saveOwnerChanges(this: DatasetOwnershipContainer, updatedOwners: Array<IOwner>): Promise<{}> {
    const result = await this.notifyOnSave(updateDatasetOwnersByUrn(get(this, 'urn'), updatedOwners));
    const { notify } = get(this, 'notifications');

    try {
      this.getDatasetOwnersTask.perform();
    } catch (e) {
      notify({ type: NotificationEvent.error, content: 'Error occurred getting updated owners.' });
    }
    return result;
  }
}
