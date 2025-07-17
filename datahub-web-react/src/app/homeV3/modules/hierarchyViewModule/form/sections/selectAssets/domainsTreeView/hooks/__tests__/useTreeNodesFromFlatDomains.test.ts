import { renderHook } from '@testing-library/react-hooks';

import useTreeNodesFromFlatDomains from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useTreeNodesFromFlatDomains';

import { Domain, EntityType } from '@types';

describe('useTreeNodesFromFlatDomains', () => {
    it('should return an empty array if domains is undefined', () => {
        const { result } = renderHook(() => useTreeNodesFromFlatDomains(undefined));
        expect(result.current).toEqual([]);
    });

    it('should convert flat domains to a tree structure', () => {
        const parentDomain: Domain = {
            urn: 'parent',
            id: 'parent',
            type: EntityType.Domain,
        };

        const domain: Domain = {
            urn: 'child',
            id: 'child',
            type: EntityType.Domain,
            parentDomains: {
                count: 1,
                domains: [parentDomain],
            },
        };

        const { result } = renderHook(() => useTreeNodesFromFlatDomains([domain]));
        expect(result.current).toEqual([
            {
                value: 'parent',
                label: 'parent',
                entity: parentDomain,
                children: [
                    {
                        value: 'child',
                        label: 'child',
                        entity: domain,
                    },
                ],
            },
        ]);
    });
});
