import { Entity } from '@types';

export interface FilterOptionType {
    field: string;
    value: string;
    count?: number;
    entity?: Entity | null;
    displayName?: string;
}
