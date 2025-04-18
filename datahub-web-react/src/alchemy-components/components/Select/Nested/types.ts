import { SelectOption } from '@components/components/Select/types';

import { Entity } from '@src/types.generated';
import { SelectOption } from '../types';

export interface NestedSelectOption extends SelectOption {
    parentValue?: string;
    isParent?: boolean;
    entity?: Entity;
}
