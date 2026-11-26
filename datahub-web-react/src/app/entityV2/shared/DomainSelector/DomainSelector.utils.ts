import { EntityNestedSelectOption } from '@app/sharedV2/select/select.types';

import { Domain } from '@types';

// Uses the option's own parentDomains rather than its position in the tree, so flat search results
// are covered too. An explicitly selected subdomain stays clickable so it can be deselected.
export function isSubdomainOfSelected(option: EntityNestedSelectOption, selectedOptions: EntityNestedSelectOption[]) {
    const selectedUrns = new Set(selectedOptions.map((selected) => selected.value));
    if (selectedUrns.has(option.value)) return false;
    const ancestors = (option.entity as Domain | undefined)?.parentDomains?.domains ?? [];
    return ancestors.some((ancestor) => selectedUrns.has(ancestor.urn));
}
