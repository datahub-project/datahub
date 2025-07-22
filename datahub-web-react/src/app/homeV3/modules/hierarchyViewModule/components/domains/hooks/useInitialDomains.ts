import useDomainsByUrns from './useDomainsByUrns';

export default function useInitialDomains(domainUrns: string[]) {
    return useDomainsByUrns(domainUrns);
}
