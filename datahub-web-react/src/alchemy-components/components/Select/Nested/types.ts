import { Entity } from '@src/types.generated';
import { BaseSelectOption } from '../types';

export interface SelectOption extends BaseSelectOption {
    parentValue?: string;
    isParent?: boolean;
    entity?: Entity;
}

export type FilteringPredicate<Option extends SelectOption = SelectOption> = (option: Option, query: string) => boolean;
