import Component from '@ember/component';
import { action } from '@ember-decorators/object';
import { Relationships, RelationshipType } from 'wherehows-web/typings/api/datasets/relationships';
import { computed, get, getProperties, set } from '@ember/object';
import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import { arrayMap, arrayPipe, arrayReduce } from 'wherehows-web/utils/array';
import ComputedProperty from '@ember/object/computed';
import {
  allRelationshipType,
  dedupeType,
  filterRelationshipsByType,
  takeNRelationships
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
   * @type {Relationships}
   * @memberof DatasetRelationshipTable
   */
  relationships: Relationships;

  /**
   * References the currently selected relationship type, used to filter out relationships
   * of non matching type
   * @type {RelationshipType}
   * @memberof DatasetRelationshipTable
   */
  selectedRelationshipType: RelationshipType;

  /**
   * Flag indicating if all relationships for the selected relationship type should be shown
   * @type {boolean}
   * @memberof DatasetRelationshipTable
   */
  showAllRelationships: boolean = false;

  constructor() {
    super(...arguments);

    // set default values for required props
    this.selectedRelationshipType || set(this, 'selectedRelationshipType', allRelationshipType);
    Array.isArray(this.relationships) || set(this, 'relationships', []);
  }

  /**
   * Computes the list of relationships to be rendered based on the currently set props
   * for filter values e.g. relationshipType and show all flag
   * @type {ComputedProperty<Relationships>}
   * @memberof DatasetRelationshipTable
   */
  filteredRelationships: ComputedProperty<Relationships> = computed(
    'selectedRelationshipType',
    'showAllRelationships',
    function(this: DatasetRelationshipTable): Relationships {
      const {
        selectedRelationshipType: { value },
        relationships,
        showAllRelationships,
        truncatedLength: n
      } = getProperties(this, ['selectedRelationshipType', 'relationships', 'showAllRelationships', 'truncatedLength']);

      return arrayPipe<Relationships>(
        filterRelationshipsByType(value), // TODO: might need to break out into separate CP, relationshipsOfTypeN for hasMore CP filteredLength parameter
        takeNRelationships(showAllRelationships, n)
      )(relationships);
    }
  );

  /**
   * Computed flag indicating if the currently rendered list of relationships has more
   * elements than the current truncation length value
   * @type {ComputedProperty<boolean>}
   * @memberof DatasetRelationshipTable
   */
  hasMore = computed(
    'showAllRelationships',
    'relationships.length',
    'truncatedLength',
    'filteredRelationships.length',
    function(this: DatasetRelationshipTable): boolean {
      const {
        selectedRelationshipType,
        relationships: { length: totalLength },
        filteredRelationships: { length: filteredLength },
        truncatedLength
      } = getProperties(this, [
        'selectedRelationshipType',
        'relationships',
        'filteredRelationships',
        'truncatedLength'
      ]);
      const hasFilter = selectedRelationshipType !== allRelationshipType;

      return hasFilter ? filteredLength > truncatedLength : totalLength > truncatedLength;
    }
  );

  /**
   * Computes a unique list of relationship types found the list of relationship
   * @type {ComputedProperty<Array<RelationshipType>>}
   * @memberof DatasetRelationshipTable
   */
  relationshipTypes: ComputedProperty<Array<RelationshipType>> = computed('relationships.[]', function(
    this: DatasetRelationshipTable
  ): Array<RelationshipType> {
    const relationships = get(this, 'relationships');
    const typeOption = ({ nativeType }: IDatasetView): RelationshipType => ({
      label: nativeType,
      value: nativeType
    });

    return [allRelationshipType, ...arrayPipe(arrayMap(typeOption), arrayReduce(dedupeType, []))(relationships)];
  });

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
