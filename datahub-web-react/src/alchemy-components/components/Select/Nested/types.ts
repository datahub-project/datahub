import { SelectOption } from '@components/components/Select/types';

export interface NestedSelectOption extends SelectOption {
    parentValue?: string;
    isParent?: boolean;
}
