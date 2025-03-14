import { EntityTypeOption } from './types';

export function entityTypeFilteringPredicate(option: EntityTypeOption, query: string): boolean {
    if (option.displayName === undefined) return false;

    return option.displayName.toLowerCase().includes(query.toLowerCase());
}
