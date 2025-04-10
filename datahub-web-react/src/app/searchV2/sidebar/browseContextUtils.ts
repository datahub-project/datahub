import { FacetFilterInput } from '../../../types.generated';
import { ENTITY_SUB_TYPE_FILTER_NAME } from '../utils/constants';

export function getEntitySubtypeFiltersForEntity(entityType: string, existingFilters: FacetFilterInput[]) {
    return existingFilters
        .find((f) => f.field === ENTITY_SUB_TYPE_FILTER_NAME)
        ?.values?.filter((value) => value.includes(entityType));
}
