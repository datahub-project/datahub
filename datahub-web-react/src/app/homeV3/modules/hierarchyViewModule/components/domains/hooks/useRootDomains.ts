import useDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useDomains';

const MAX_ROOT_DOMAINS = 1000;

export default function useRootDomains() {
    return useDomains(undefined, 0, MAX_ROOT_DOMAINS, false);
}
