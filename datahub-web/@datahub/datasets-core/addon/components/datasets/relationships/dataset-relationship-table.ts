import Component from '@ember/component';
// @ts-ignore
import template from '../../../templates/components/datasets/relationships/dataset-relationship-table';
import { layout } from '@ember-decorators/component';
import { action, computed } from '@ember/object';
import { IDatasetLineage, DatasetLineageList } from '@datahub/metadata-types/types/entity/dataset/lineage';
import { set } from '@ember/object';
import { arrayMap, arrayPipe, arrayReduce } from '@datahub/utils/array/index';
import {
  allRelationshipType,
  dedupeType,
  takeNLineageItems,
  filterLineageByType
} from '@datahub/datasets-core/utils/lineage';
import { AppRoute } from '@datahub/data-models/types/entity/shared';
import { INachoDropdownOption } from '@nacho-ui/core/types/nacho-dropdown';

/**
 * Shortcut typing to reference dropdown options for relationship types
 */
type RelationshipType = INachoDropdownOption<string>;

@layout(template)
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
  relationships: DatasetLineageList = [];

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
  showAllRelationships = false;

  @computed('selectedRelationshipType', 'relationships.[]')
  get filteredRelationshipsByType(): DatasetLineageList {
    return filterLineageByType(this.selectedRelationshipType.value)(this.relationships);
  }

  /**
   * Determines if a supplied urn string is an actor that can be linked to within the DataHub app or externally
   * @private
   * @param {string} actorUrn the urn for the actor in the dataset relationship
   */
  protected linkableActor(_actorUrn?: string): { route: AppRoute | null; actorType?: string } {
    return { route: null };
  }

  /**
   * Computes the list of relationships to be rendered based on the currently set props
   * for filter values e.g. relationshipType and show all flag
   * @type {Relationships}
   * @memberof DatasetRelationshipTable
   */
  @computed('showAllRelationships', 'filteredRelationshipsByType')
  get filteredRelationships(): DatasetLineageList & Array<ReturnType<DatasetRelationshipTable['linkableActor']>> {
    const { filteredRelationshipsByType, showAllRelationships, truncatedLength: n } = this;

    return takeNLineageItems(
      showAllRelationships,
      n
    )(filteredRelationshipsByType).map(({ actor, ...lineage }): IDatasetLineage &
      ReturnType<DatasetRelationshipTable['linkableActor']> => ({
      actor,
      ...lineage,
      ...this.linkableActor(actor)
    }));
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
   */
  @action
  onToggleShowMore(): void {
    this.toggleProperty('showAllRelationships');
  }
}
