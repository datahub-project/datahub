import { Entity } from '../../../types.generated';

export interface FilterOptionType {
    field: string;
    value: string;
    count?: number;
    entity?: Entity | null;
    displayName?: string;
}
