import { Entity } from '@src/types.generated';

export interface SelectOption {
    value: string;
    label: string;
    parentValue?: string;
    isParent?: boolean;
    entity?: Entity;
}
