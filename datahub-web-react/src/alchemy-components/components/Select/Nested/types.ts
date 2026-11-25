import { SelectOption } from '@components/components/Select/types';

export interface NestedSelectOption extends SelectOption {
    parentValue?: string;
    isParent?: boolean;
    // Overrides the value in the option's data-testid when the value is an internal encoding.
    testId?: string;
}
