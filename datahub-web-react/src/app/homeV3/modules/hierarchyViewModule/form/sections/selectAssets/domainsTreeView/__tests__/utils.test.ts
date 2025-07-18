import { describe, expect, it, vi } from 'vitest';

import { DomainItem } from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/types';
import {
    convertDomainToTreeNode,
    unwrapFlatDomainsToTreeNodes,
} from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/utils';
import * as utils from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/utils';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

import { Domain } from '@types';

// Mock the utility module
vi.mock('@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/utils', () => ({
    unwrapParentEntitiesToTreeNodes: vi.fn(),
}));

describe('domainUtils', () => {
    describe('convertDomainToTreeNode', () => {
        it('converts domain without children correctly', () => {
            const domain: DomainItem = {
                urn: 'domain:1',
                children: { total: 0 },
            } as DomainItem;

            const result = convertDomainToTreeNode(domain);

            expect(result).toEqual({
                value: 'domain:1',
                label: 'domain:1',
                hasAsyncChildren: false,
                totalChildren: 0,
                entity: domain,
            });
        });

        it('converts domain with children correctly', () => {
            const domain: DomainItem = {
                urn: 'domain:2',
                children: { total: 5 },
            } as DomainItem;

            const result = convertDomainToTreeNode(domain);

            expect(result).toEqual({
                value: 'domain:2',
                label: 'domain:2',
                hasAsyncChildren: true,
                totalChildren: 5,
                entity: domain,
            });
        });
    });

    describe('unwrapFlatDomainsToTreeNodes', () => {
        it('calls unwrapParentEntitiesToTreeNodes with correct arguments', () => {
            const domains = [
                { urn: 'domain:1', parentDomains: { domains: [] } },
                { urn: 'domain:2', parentDomains: null },
            ] as Domain[];

            // Setup mock implementation
            vi.mocked(utils.unwrapParentEntitiesToTreeNodes).mockReturnValue([]);

            unwrapFlatDomainsToTreeNodes(domains);

            // Verify correct domains are passed
            expect(utils.unwrapParentEntitiesToTreeNodes).toHaveBeenCalledWith(domains, expect.any(Function));

            // Verify parent accessor function
            const parentAccessor = vi.mocked(utils.unwrapParentEntitiesToTreeNodes).mock.calls[0][1];

            // Test domain with parents
            const domainWithParents = {
                urn: 'child-domain',
                parentDomains: { domains: [{ urn: 'parent1' }, { urn: 'parent2' }] },
            } as Domain;
            expect(parentAccessor(domainWithParents)).toEqual([{ urn: 'parent2' }, { urn: 'parent1' }]);

            // Test domain without parents
            const domainWithoutParents = { urn: 'orphan', parentDomains: null } as Domain;
            expect(parentAccessor(domainWithoutParents)).toEqual([]);
        });

        it('returns result from unwrapParentEntitiesToTreeNodes', () => {
            const mockTreeNodes = [{ value: 'test' }] as TreeNode[];
            vi.mocked(utils.unwrapParentEntitiesToTreeNodes).mockReturnValue(mockTreeNodes);

            const result = unwrapFlatDomainsToTreeNodes([]);
            expect(result).toBe(mockTreeNodes);
        });
    });
});
