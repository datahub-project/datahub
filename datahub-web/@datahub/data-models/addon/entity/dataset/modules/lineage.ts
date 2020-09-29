import { IDatasetLineage } from '@datahub/metadata-types/types/entity/dataset/lineage';
import { computed } from '@ember/object';
import { oneWay } from '@ember/object/computed';

/**
 * The dataset lineage object is a light wrapper around the dataset lineage API response to provide the
 * expected data layer for the upstream or downstream dataset interface
 */
export class DatasetLineage {
  /**
   * Holds the originally fetched API data for this lineage object. It should be the source of truth and
   * untouched by the UI
   * @type {IDatasetLineage}
   */
  private readonly data: IDatasetLineage;

  /**
   * Provides a raw dataset data for the underlying lineage piece, with the addition of a urn property, intended to be
   * utilized by our data model service to be able to generically instantiate a dataset entity out of the object
   * @note WARNING - do not use this property directly. Because it contains raw API data that can differ between open
   * source and an internal implementation, using properties directly from this referenced object can have unintended
   * consequences
   */
  @computed('data')
  get _rawDatasetData(): IDatasetLineage['dataset'] & { urn: string } {
    return { ...this.data.dataset, urn: this.urn };
  }

  /**
   * Gets the urn of the related dataset entity
   */
  @computed('data')
  get urn(): string {
    return this.data.dataset.uri;
  }

  /**
   * Reads the actor value from the retrieved data from the lineage object
   * @type {string}
   */
  @oneWay('data.actor')
  actor!: string;

  /**
   * Reads the type of lineage value from the retrieved data from the lineage object
   * @type {string}
   */
  @oneWay('data.type')
  type!: string;

  constructor(lineage: IDatasetLineage) {
    this.data = lineage;
  }
}
