import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { reads } from '@ember/object/computed';
import { task } from 'ember-concurrency';
import { ETask } from '@datahub/utils/types/concurrency';
import { IOwnerResponse, IOwner } from '@datahub/data-models/types/entity/dataset/ownership';
import { readDatasetOwnersByUrn } from '@datahub/data-models/api/dataset/ownership';
import { setProperties, computed } from '@ember/object';

/**
 * TODO: META-10994 Finish refactor Dataset ownership modeling in DataHub
 */

/**
 * Module util checks if the ownership of a dataset is valid by comparing against a set of criteria
 * - The owner must be confirmed by another user,
 * - The owner type must be of category DATA_OWNER (DataOwner at mid-tier)
 * - The owner must be of idType USER
 * - The owner must be active
 * @param {IOwner} { confirmedBy, type, idType, isActive }
 */
const isValidConfirmedOwner = ({ confirmedBy, type, idType, isActive }: IOwner): boolean =>
  Boolean(confirmedBy) && ['DATA_OWNER', 'DataOwner'].includes(type) && idType === 'USER' && isActive;

/**
 * DatasetOwnership models the ownership attributes and behavior for a dataset
 * Instantiated with a reference to the DatasetEntity and utilized a builder to reify ownership
 * attributes
 * @export
 * @class DatasetOwnership
 */
export default class DatasetOwnership {
  /**
   * The minimum required number of valid confirmed owners
   * @static
   */
  static readonly minRequiredConfirmedOwners = 2;

  /**
   * The list of owners associated with the dataset
   */
  owners: Array<IOwner> = [];

  /**
   * Flag indicates if the dataset is from an upstream source
   */
  fromUpstream = false;

  /**
   * Dataset urn, should match the urn attribute on the dataset unless, fromUpstream is true, then
   * upstreamUrn will the upstream dataset
   */
  upstreamUrn = '';

  /**
   * When the dataset's ownership was last modified
   */
  lastModified?: number;

  /**
   * The entity that last modified the ownership information
   */
  actor = '';

  /**
   * References the urn for the associated dataset, definitely assigned since a dataset is required to
   * instantiate DatasetOwnership
   */
  @reads('entity.urn')
  urn!: string;

  /**
   * Computed flag in if the ownership information for the dataset meets the requirements
   * @readonly
   */
  @computed('owners')
  get isValid(): boolean {
    return this.owners.filter(isValidConfirmedOwner).length >= DatasetOwnership.minRequiredConfirmedOwners;
  }

  /**
   * Checks if the userName is listed as an owner on the dataset ownership information
   * @param {string} userName the userName to match against in the list of owners
   */
  isOwner(userName: string): boolean {
    return Boolean(this.owners.findBy('userName', userName));
  }

  /**
   * Requests the ownership information for the dataset and sets the associated DatasetOwnership attributes
   */
  @task(function*(this: DatasetOwnership): IterableIterator<Promise<IOwnerResponse>> {
    const { owners = [], fromUpstream, datasetUrn, lastModified, actor } = ((yield readDatasetOwnersByUrn(
      this.urn
    )) as unknown) as IOwnerResponse;

    setProperties(this, { owners, fromUpstream, upstreamUrn: datasetUrn, lastModified, actor });
  })
  getDatasetOwnersTask!: ETask<IOwnerResponse>;

  /**
   * Class builder allows async instantiation / creation operations to be handled post instantiation
   */
  async build(): Promise<this> {
    await this.getDatasetOwnersTask.perform();

    return this;
  }

  /**
   * Creates an instance of DatasetOwnership. To reify properties the build method should be invoked post instantiation
   * @param {DatasetEntity} entity the DatasetEntity for which ownership information is requested
   */
  constructor(readonly entity: DatasetEntity) {}
}
