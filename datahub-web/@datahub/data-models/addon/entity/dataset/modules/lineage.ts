import { DatasetEntity } from '../dataset-entity';
import { IDatasetLineage } from '@datahub/metadata-types/types/entity/dataset/lineage';
import { computed } from '@ember/object';
import { oneWay } from '@ember/object/computed';
import { fromLegacy } from '@datahub/data-models/entity/dataset/utils/legacy';

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
   * Creates a new dataset entity object from the retrieved data for this lineage object. We can create a
   * dataset entity object from this as the interface of the dataset property on the lineage object is
   * expected to be the same as the dataset readEntity response
   * @type {DatasetEntity}
   */
  @computed('data')
  get dataset(): DatasetEntity {
    const { dataset } = this.data;
    return new DatasetEntity(dataset.uri, fromLegacy(dataset));
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
