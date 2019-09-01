import Component from '@ember/component';
import { action, computed } from '@ember/object';
import { RelationshipType, IDatasetLineage, LineageList } from 'wherehows-web/typings/api/datasets/relationships';
import { set } from '@ember/object';
import { arrayMap, arrayPipe, arrayReduce } from 'wherehows-web/utils/array';
import {
  allRelationshipType,
  dedupeType,
  takeNLineageItems,
  filterLineageByType
} from 'wherehows-web/utils/datasets/lineage';

export default class DatasetRelationshipTable extends Component {
  /**
   * The default number of element relationships to render initially
   * @type {number}
   * @memberof DatasetRelationshipTable
   */
  truncatedLength = 10;

  /**
   * List of  dataset relationships
   * @type {IDatasetLineage}
   * @memberof DatasetRelationshipTable
   */
  relationships: LineageList = [];

  /**
   * References the currently selected relationship type, used to filter out relationships
   * of non matching type
   * @type {RelationshipType}
   * @memberof DatasetRelationshipTable
   */
  selectedRelationshipType: RelationshipType = allRelationshipType;

  /**
   * Flag indicating if all relationships for the selected relationship type should be shown
   * @type {boolean}
   * @memberof DatasetRelationshipTable
   */
  showAllRelationships: boolean = false;

  @computed('selectedRelationshipType', 'relationships.[]')
  get filteredRelationshipsByType(): LineageList {
    return filterLineageByType(this.selectedRelationshipType.value)(this.relationships);
  }

  /**
   * Computes the list of relationships to be rendered based on the currently set props
   * for filter values e.g. relationshipType and show all flag
   * @type {Relationships}
   * @memberof DatasetRelationshipTable
   */
  @computed('showAllRelationships', 'filteredRelationshipsByType')
  get filteredRelationships(): LineageList {
    const { filteredRelationshipsByType, showAllRelationships, truncatedLength: n } = this;

    return takeNLineageItems(showAllRelationships, n)(filteredRelationshipsByType);
  }

  /**
   * Computed flag indicating if the currently rendered list of relationships has more
   * elements than the current truncation length value
   * @type {boolean}
   * @memberof DatasetRelationshipTable
   */
  @computed('showAllRelationships', 'relationships.length', 'truncatedLength', 'filteredRelationshipsByType.length')
  get hasMore(): boolean {
    const {
      selectedRelationshipType,
      relationships: { length: totalLength },
      filteredRelationshipsByType: { length: filteredLength },
      truncatedLength
    } = this;
    const hasFilter = selectedRelationshipType !== allRelationshipType;

    return hasFilter ? filteredLength > truncatedLength : totalLength > truncatedLength;
  }

  /**
   * Computes a unique list of relationship types found the list of relationship
   * @type {Array<RelationshipType>}
   * @memberof DatasetRelationshipTable
   */
  @computed('relationships.[]')
  get relationshipTypes(): Array<RelationshipType> {
    const typeOption = ({ type }: IDatasetLineage): RelationshipType => ({
      label: type,
      value: type
    });
    const createTypeOptionAndDedupe = arrayPipe(arrayMap(typeOption), arrayReduce(dedupeType, []));

    return [allRelationshipType, ...createTypeOptionAndDedupe(this.relationships)];
  }

  /**
   * Handles the relationship type selection change
   * @param {RelationshipType} type
   * @memberof DatasetRelationshipTable
   */
  @action
  onTypeSelect(type: RelationshipType): void {
    set(this, 'selectedRelationshipType', type);
  }

  /**
   * Toggles the flag to show all available relationships
   * @memberof DatasetRelationshipTable
   */
  @action
  onToggleShowMore() {
    this.toggleProperty('showAllRelationships');
  }
}
