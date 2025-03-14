import { SelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { isItDomainEntity } from '@src/app/entityV2/domain/utils';
import { Domain } from '@src/types.generated';
import { getUniqueItemsByKey } from '../../utils';

export function domainKeyAccessor(domain: Domain) {
    return domain.urn;
}

export function extractParentDomains(domains: Domain[]) {
    const allParentDomains = domains
        .map((domain) => domain.parentDomains?.domains ?? [])
        .map((arrayOfParentDomains) => arrayOfParentDomains.filter(isItDomainEntity))
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

export function domainFilteringPredicate(option: SelectOption, query: string) {
    const { entity } = option;
    if (!isItDomainEntity(entity)) return false;

    const searchText = (entity.properties?.name ?? '').toLowerCase();
    return searchText.includes(query.toLowerCase()) || entity.urn.toLowerCase().includes(query.toLowerCase());
}
