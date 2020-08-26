import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { capitalize } from '@ember/string';
import { arrayToString } from '@datahub/utils/array/array-to-string';

/**
 * Outputs the list of available entities readable by humans, eg: Datasets or UMP Metrics
 */
export const stringListOfEntities = (entities: Array<DataModelEntity>): string =>
  arrayToString(entities.map((entity: DataModelEntity) => capitalize(entity.displayName)));
