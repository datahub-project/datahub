import { isSubdomainOfSelected } from '@app/entityV2/shared/DomainSelector/DomainSelector.utils';
import { EntityNestedSelectOption } from '@app/sharedV2/select/select.types';

import { Domain, EntityType } from '@types';

function domainOption(urn: string, ancestors: string[] = []): EntityNestedSelectOption {
    const entity = {
        urn,
        type: EntityType.Domain,
        parentDomains: { count: ancestors.length, domains: ancestors.map((ancestor) => ({ urn: ancestor })) },
    } as unknown as Domain;
    return { value: urn, label: urn, entity };
}

describe('isSubdomainOfSelected', () => {
    const parent = domainOption('urn:li:domain:parent');
    const grandchild = domainOption('urn:li:domain:grandchild', ['urn:li:domain:child', 'urn:li:domain:parent']);

    it('covers a subdomain at any depth below a selected domain', () => {
        expect(isSubdomainOfSelected(grandchild, [parent])).toBe(true);
    });

    it('does not cover a domain whose ancestors are all unselected', () => {
        expect(isSubdomainOfSelected(grandchild, [domainOption('urn:li:domain:other')])).toBe(false);
    });

    it('leaves an explicitly selected subdomain clickable', () => {
        expect(isSubdomainOfSelected(grandchild, [parent, grandchild])).toBe(false);
    });
});
