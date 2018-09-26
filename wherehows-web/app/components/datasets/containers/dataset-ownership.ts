import Component from '@ember/component';
import { get, set, getProperties, setProperties } from '@ember/object';
import { task, TaskInstance } from 'ember-concurrency';
import { action, computed } from '@ember-decorators/object';
import Notifications from 'wherehows-web/services/notifications';
import { NotificationEvent } from 'wherehows-web/services/notifications';
import { IOwner, IOwnerResponse } from 'wherehows-web/typings/api/datasets/owners';
import {
  OwnerType,
  readDatasetOwnersByUrn,
  readDatasetSuggestedOwnersByUrn,
  readDatasetOwnerTypesWithoutConsumer,
  updateDatasetOwnersByUrn
} from 'wherehows-web/utils/api/datasets/owners';
import { service } from '@ember-decorators/service';
import { IAppConfig } from 'wherehows-web/typings/api/configurator/configurator';
import Configurator from 'wherehows-web/services/configurator';
import { containerDataSource } from 'wherehows-web/utils/components/containers/data-source';

@containerDataSource('getContainerDataTask')
export default class DatasetOwnershipContainer extends Component {
  /**
   * The urn identifier for the dataset
   * @type {string}
   */
  urn: string;

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
   * An async parent task to group all data tasks for this container component
   * @type {Task<TaskInstance<Promise<any>>, (a?: any) => TaskInstance<TaskInstance<Promise<any>>>>}
   */
  getContainerDataTask = task(function*(
    this: DatasetOwnershipContainer
  ): IterableIterator<TaskInstance<Promise<any> | IAppConfig['userEntityProps']>> {
    const tasks = Object.values(
      getProperties(this, [
        'getDatasetOwnersTask',
        'getSuggestedOwnersTask',
        'getDatasetOwnerTypesTask',
        'getAvatarProperties'
      ])
    );

    yield* tasks.map(task => task.perform());
  });

  /**
   * Fetches & sets avatar props to build owner avatar images
   * @memberof DatasetOwnershipContainer
   */
  getAvatarProperties = task(function*(
    this: DatasetOwnershipContainer
  ): IterableIterator<IAppConfig['userEntityProps']> {
    return set(this, 'avatarProperties', Configurator.getConfig('userEntityProps'));
  });

  /**
   * Reads the owners for this dataset
   * @type {Task<Promise<Array<IOwner>>, (a?: any) => TaskInstance<Promise<IOwnerResponse>>>}
   */
  getDatasetOwnersTask = task(function*(this: DatasetOwnershipContainer): IterableIterator<Promise<IOwnerResponse>> {
    const { owners = [], fromUpstream, datasetUrn }: IOwnerResponse = yield readDatasetOwnersByUrn(get(this, 'urn'));

    setProperties(this, { owners, fromUpstream, upstreamUrn: datasetUrn });
  });

  /**
   * Retrieves metadata about the current ownership records including modificationTime and actor
   * @readonly
   * @type {(Record<'actor' | 'modificationTime', string>)}
   * @memberof DatasetOwnershipContainer
   */
  @computed('owners.[]')
  get ownershipMetadata(): Record<'actor' | 'modificationTime', string> {
    const {
      owners: [owner]
    } = this;
    const ownershipMetadata = { actor: '', modificationTime: '' };

    if (owner) {
      const { confirmedBy, modifiedTime } = owner;
      const modificationTime = modifiedTime ? String(modifiedTime) : '';

      return { ...ownershipMetadata, actor: confirmedBy || '', modificationTime };
    }

    return ownershipMetadata;
  }

  /**
   * Fetches the suggested owners for this dataset
   * @type {Task<Promise<Array<IOwner>>, (a?: any) => TaskInstance<Promise<IOwnerResponse>>>}
   */
  getSuggestedOwnersTask = task(function*(this: DatasetOwnershipContainer): IterableIterator<Promise<IOwnerResponse>> {
    const { owners = [] }: IOwnerResponse = yield readDatasetSuggestedOwnersByUrn(this.urn);

    setProperties(this, { suggestedOwners: owners });
  });

  /**
   * Reads the owner types available
   * @type {Task<Promise<Array<OwnerType>>, (a?: any) => TaskInstance<Promise<Array<OwnerType>>>>}
   */
  getDatasetOwnerTypesTask = task(function*(
    this: DatasetOwnershipContainer
  ): IterableIterator<Promise<Array<OwnerType>>> {
    const ownerTypes: Array<OwnerType> = yield readDatasetOwnerTypesWithoutConsumer();
    set(this, 'ownerTypes', ownerTypes);
  });

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
      notify(NotificationEvent.success, { content: 'Changes have been successfully saved!' });
    } catch (e) {
      notify(NotificationEvent.error, { content: 'An error occurred while saving.' });
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
    const result = await this.notifyOnSave(updateDatasetOwnersByUrn(get(this, 'urn'), '', updatedOwners));
    const { notify } = get(this, 'notifications');

    try {
      get(this, 'getDatasetOwnersTask').perform();
    } catch (e) {
      notify(NotificationEvent.error, { content: 'Error occurred getting updated owners.' });
    }
    return result;
  }
}
