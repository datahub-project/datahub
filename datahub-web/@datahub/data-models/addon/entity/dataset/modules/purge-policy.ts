import { IDatasetRetentionPolicy } from '@datahub/metadata-types/types/entity/dataset/compliance/retention';
import { alias } from '@ember/object/computed';
import { setProperties } from '@ember/object';
import { PurgePolicy } from '@datahub/metadata-types/constants/entity/dataset/compliance/purge-policy';

export class DatasetPurgePolicy {
  /**
   * Original API data retrieved for this class. Stored here to power our working copy of the
   * purge policy
   * @type {IDatasetRetentionPolicy}
   * @memberof DatasetPurgePolicy
   */
  readonly data?: IDatasetRetentionPolicy;

  /**
   * optional string with username of modifier
   * @type {string}
   * @memberof DatasetPurgePolicy
   */
  @alias('data.modifiedBy')
  modifiedBy?: string;

  /**
   * optional timestamp of last modification date
   * @type {number}
   * @memberof DatasetPurgePolicy
   */
  @alias('data.modifiedTime')
  modifiedTime?: number;

  /**
   * User entered purge notation for a dataset with a purge exempt policy
   * @type {string | null}
   * @memberof DatasetPurgePolicy
   */
  purgeNote!: string | null;

  /**
   * Purge Policy for the dataset
   * @type {PurgePolicy | ''}
   * @memberof DatasetPurgePolicy
   */
  purgeType!: PurgePolicy | '';

  /**
   * Creates a working copy for the purge policy from the data that instantiated this class.
   */
  createWorkingCopy(): void {
    const { data } = this;
    const { purgeNote = null, purgeType = '' }: Partial<Pick<IDatasetRetentionPolicy, 'purgeNote' | 'purgeType'>> =
      data || {};
    setProperties(this, { purgeNote, purgeType });
  }

  /**
   * Creates an API friendly object from our classified working copy.
   */
  readWorkingCopy(): Pick<IDatasetRetentionPolicy, 'purgeNote' | 'purgeType'> {
    const { purgeNote, purgeType } = this;
    return { purgeNote, purgeType };
  }

  constructor(purgePolicyData?: IDatasetRetentionPolicy) {
    this.data = purgePolicyData;
    this.createWorkingCopy();
  }
}
