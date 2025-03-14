import { SelectOption } from '@src/alchemy-components/components/Select/Nested/types';

export interface EntityTypeOption extends SelectOption {
    // this value is used in `filteringPredicate` to filter options in `NestedSelect`
    displayName?: string;
}
