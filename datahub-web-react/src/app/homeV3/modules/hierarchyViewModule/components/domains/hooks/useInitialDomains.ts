import useDomainsByUrns from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useDomainsByUrns';

export default function useInitialDomains(domainUrns: string[]) {
    return useDomainsByUrns(domainUrns);
}
