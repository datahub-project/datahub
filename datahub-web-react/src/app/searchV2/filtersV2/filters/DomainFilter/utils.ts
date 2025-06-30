import { getUniqueItemsByKey } from '@app/searchV2/filtersV2/utils';
import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { isDomain } from '@src/app/entityV2/domain/utils';
import { Domain } from '@src/types.generated';

export function domainKeyAccessor(domain: Domain) {
    return domain.urn;
}

export function extractParentDomains(domains: Domain[]) {
    const allParentDomains = domains
        .map((domain) => domain.parentDomains?.domains ?? [])
        .map((arrayOfParentDomains) => arrayOfParentDomains.filter(isDomain))
        .map((arrayOfParentDomains) => arrayOfParentDomains.reverse())
        .map((arrayOfParentDomains) =>
            arrayOfParentDomains.reduce(
                (parentDomains, domain) => [
                    {
                        ...domain,
                        parentDomains: { count: parentDomains.length, domains: parentDomains },
                    },
                    ...parentDomains,
                ],
                [] as Domain[],
            ),
        )
        .flat();

    return getUniqueItemsByKey(allParentDomains, domainKeyAccessor);
}

export function domainFilteringPredicate(option: NestedSelectOption, query: string) {
    const { entity } = option;
    if (!isDomain(entity)) return false;

    const searchText = (entity.properties?.name ?? '').toLowerCase();
    return searchText.includes(query.toLowerCase()) || entity.urn.toLowerCase().includes(query.toLowerCase());
}
