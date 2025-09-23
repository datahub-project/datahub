import { SelectOption } from '@src/alchemy-components';
import { Entity } from '@src/types.generated';

export interface BaseEntitySelectOption extends SelectOption {
    entity: Entity; // stored in option to pass it in applied filters and forcibly show options from applied filters
    displayName: string; // for filtering by name
}
