import { INachoDropdownOption } from '@nacho-ui/dropdown/types/nacho-dropdown';
import { IDatasetEntity } from '@datahub/metadata-types/types/entity/dataset/dataset-entity';

// TODO: [META-8686] These lineage API items are still in place here as there is some difference
// in the typings and the actual handling that make the two currently incompatible with a simple
// reference change. Refer to @datahub/data-models/api/dataset/lineage for the maintained version
// that these should be migrated to

/**
 * Alias for a drop-down option based on an IDatasetEntity nativeType
 * @alias
 * @type RelationshipType
 */
export type RelationshipType = INachoDropdownOption<string>;

/**
 * Relationship upstream and downstream api will return Array<IDatasetLineage> displaying
 * the type of dataset, type of lineage, and actor urn that modified that relationship
 */
export interface IDatasetLineage {
  dataset: IDatasetEntity;
  type: string;
  actor: string;
}

export type LineageList = Array<IDatasetLineage>;
