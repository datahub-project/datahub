import { act } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';

import useDomainsTreeViewState from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useDomainsTreeViewState';
import useInitialDomains from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useInitialDomains';
import useRootDomains from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useRootDomains';
import useTreeNodesFromFlatDomains from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useTreeNodesFromFlatDomains';
import useTreeNodesFromDomains from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useTreeNodesFromListDomains';
import { mergeTrees } from '@app/homeV3/modules/hierarchyViewModule/treeView/utils';

vi.mock('@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useInitialDomains');
vi.mock('@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useRootDomains');
vi.mock(
    '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useTreeNodesFromFlatDomains',
);
vi.mock(
    '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useTreeNodesFromListDomains',
);
vi.mock('@app/homeV3/modules/hierarchyViewModule/treeView/utils', () => ({
    mergeTrees: vi.fn(),
}));

describe('useDomainsTreeViewState', () => {
    const mockInitialDomains = [{ urn: 'initialDomain1' }];
    const mockRootDomains = [{ urn: 'rootDomain1' }];
    const mockInitialTreeNodes = [{ value: 'initialDomain1', label: 'initialDomain1' }];
    const mockRootTreeNodes = [{ value: 'rootDomain1', label: 'rootDomain1' }];
    const mergedTree = [{ value: 'mergedDomain', label: 'mergedDomain' }];

    beforeEach(() => {
        vi.clearAllMocks();
        vi.mocked(useInitialDomains).mockReturnValue({ domains: mockInitialDomains } as any);
        vi.mocked(useRootDomains).mockReturnValue({ domains: mockRootDomains } as any);
        vi.mocked(useTreeNodesFromFlatDomains).mockReturnValue(mockInitialTreeNodes as any);
        vi.mocked(useTreeNodesFromDomains).mockReturnValue(mockRootTreeNodes as any);
        vi.mocked(mergeTrees).mockReturnValue(mergedTree as any);
    });

    test('should call hooks with correct parameters', () => {
        const initialSelectedDomainUrns = ['domain1'];
        renderHook(() => useDomainsTreeViewState(initialSelectedDomainUrns));

        expect(useInitialDomains).toHaveBeenCalledWith(initialSelectedDomainUrns);
        expect(useRootDomains).toHaveBeenCalled();
        expect(useTreeNodesFromFlatDomains).toHaveBeenCalledWith(mockInitialDomains);
        expect(useTreeNodesFromDomains).toHaveBeenCalledWith(mockRootDomains);
    });

    test('should initialize state correctly', () => {
        const { result } = renderHook(() => useDomainsTreeViewState(['domain1']));

        expect(result.current.nodes).toEqual(mergedTree);
        expect(result.current.selectedValues).toEqual(['domain1']);
        expect(result.current.loading).toBe(false);
    });

    test('should merge trees when both domains are available', () => {
        const { result } = renderHook(() => useDomainsTreeViewState(['domain1']));

        expect(mergeTrees).toHaveBeenCalledWith(mockRootTreeNodes, mockInitialTreeNodes);
        expect(result.current.nodes).toEqual(mergedTree);
        expect(result.current.loading).toBe(false);
    });

    test('should not merge trees when either domain is undefined', () => {
        vi.mocked(useInitialDomains).mockReturnValueOnce({ domains: undefined } as any);
        vi.mocked(useRootDomains).mockReturnValueOnce({ domains: undefined } as any);

        const { result } = renderHook(() => useDomainsTreeViewState(['domain1']));

        expect(mergeTrees).not.toHaveBeenCalled();
        expect(result.current.nodes).toEqual([]);
        expect(result.current.loading).toBe(true);
    });

    test('should update selectedValues correctly', () => {
        const { result } = renderHook(() => useDomainsTreeViewState(['domain1']));

        act(() => {
            result.current.setSelectedValues(['newDomain']);
        });

        expect(result.current.selectedValues).toEqual(['newDomain']);
    });

    test('should not re-merge trees after initialization', () => {
        const { result, rerender } = renderHook(() => useDomainsTreeViewState(['domain1']));
        expect(result.current.nodes).toEqual(mergedTree);

        rerender();
        expect(mergeTrees).toHaveBeenCalledTimes(1); // Only once on mount
    });

    test('should handle empty initialSelectedDomainUrns', () => {
        const { result } = renderHook(() => useDomainsTreeViewState([]));

        expect(result.current.selectedValues).toEqual([]);
    });

    test('should handle null initialSelectedDomainUrns', () => {
        const { result } = renderHook(() => useDomainsTreeViewState(null as any));

        expect(result.current.selectedValues).toEqual([]);
    });
});
