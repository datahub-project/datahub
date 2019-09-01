import Controller from '@ember/controller';
import { setProperties } from '@ember/object';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';

export default class SearchController extends Controller {
  queryParams = ['entity', 'page', 'facets', 'keyword'];

  /**
   * The category to narrow/ filter search results
   * @type {string}
   */
  entity?: string = DatasetEntity.displayName;

  /**
   * Encoded facets state in a restli fashion
   */
  facets: string;

  /**
   * The current search page
   * @type {number}
   */
  page = 1;

  /**
   * Will clean previous data
   */
  resetData(): void {
    setProperties(this, {
      page: 1,
      facets: '',
      entity: DatasetEntity.displayName
    });
  }
}
