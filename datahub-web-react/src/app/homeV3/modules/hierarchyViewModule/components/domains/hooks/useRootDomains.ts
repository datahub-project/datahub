import useDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useDomains';

export default function useRootDomains() {
    return useDomains(undefined, 0, 5, false);
}
