import { Entity } from '../../../types.generated';

export interface FilterOptionType {
    field: string;
    value: string;
    count?: number;
    entity?: Entity | null; // TODO: If the entity is not provided, we should hydrate it.
}
