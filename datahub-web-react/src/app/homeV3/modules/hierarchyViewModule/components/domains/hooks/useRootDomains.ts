import useDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useDomains';

export default function useRootDomains(count: number) {
    return useDomains(undefined, 0, count, false);
}
