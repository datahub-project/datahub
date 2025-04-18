import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';

export interface EntityTypeOption extends NestedSelectOption {
    // this value is used in `filteringPredicate` to filter options in `NestedSelect`
    displayName?: string;
}
