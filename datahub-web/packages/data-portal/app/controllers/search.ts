import Controller from '@ember/controller';
import { setProperties, action } from '@ember/object';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { DataModelName } from '@datahub/data-models/constants/entity';

export default class SearchController extends Controller {
  queryParams = ['entity', 'page', 'facets', 'keyword'];

  /**
   * The category to narrow/ filter search results
   */
  entity?: DataModelName = DatasetEntity.displayName;

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

  /**
   * Changes our search context to a new entity and resets the current page (to avoid variable leak)
   * @param {DataModelName} newEntity - the name of the entity to change our search context to
   */
  @action
  onChangeEntity(newEntity: DataModelName): void {
    setProperties(this, {
      entity: newEntity,
      page: 1,
      facets: ''
    });
  }
}
