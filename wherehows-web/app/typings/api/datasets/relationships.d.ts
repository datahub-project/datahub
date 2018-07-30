import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import { IDropDownOption } from 'wherehows-web/typings/app/dataset-compliance';

/**
 * Alias for a list of IDatasetView instances
 * @alias
 * @type Relationships
 */
type Relationships = Array<IDatasetView>;

/**
 * Alias for a drop-down option based on an IDatasetView nativeType
 * @alias
 * @type RelationshipType
 */
type RelationshipType = IDropDownOption<string>;

export { Relationships, RelationshipType };
