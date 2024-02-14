import { FacetFilterInput } from '../../../types.generated';
import { ADVANCED_SEARCH_ONLY_FILTERS, UnionType } from './constants';

// utility method that looks at the set of filters and determines if the filters can be represented by simple search
export const hasAdvancedFilters = (filters: FacetFilterInput[], unionType: UnionType) => {
    return (
        filters.filter(
            (filter) =>
                ADVANCED_SEARCH_ONLY_FILTERS.indexOf(filter.field) >= 0 || filter.negated || unionType === UnionType.OR,
        ).length > 0
    );
};
