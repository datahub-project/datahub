import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useTreeNodesFromGlossaryTerms from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useTreeNodesFromGlossaryTerms';
import { convertGlossaryTermToTreeNode } from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/utils';

import { EntityType, GlossaryTerm } from '@types';

// Mock the conversion function
vi.mock('../../utils', () => ({
    convertGlossaryTermToTreeNode: vi.fn(),
}));

describe('useTreeNodesFromGlossaryTerms', () => {
    const mockTerm1: GlossaryTerm = {
        urn: 'term:1',
        name: 'Test Term 1',
        hierarchicalName: 'Test Term 1',
        type: EntityType.GlossaryTerm,
    };

    const mockTerm2: GlossaryTerm = {
        urn: 'term:2',
        name: 'Test Term 2',
        hierarchicalName: 'Test Term 2',
        type: EntityType.GlossaryTerm,
    };

    const mockTreeNode1 = {
        value: 'term:1',
        label: 'term:1',
        entity: mockTerm1,
    };

    const mockTreeNode2 = {
        value: 'term:2',
        label: 'term:2',
        entity: mockTerm2,
    };

    beforeEach(() => {
        vi.clearAllMocks();
        vi.mocked(convertGlossaryTermToTreeNode)
            .mockImplementationOnce(() => mockTreeNode1)
            .mockImplementationOnce(() => mockTreeNode2);
    });

    it('returns empty array when no glossary terms provided', () => {
        const { result } = renderHook(() => useTreeNodesFromGlossaryTerms());

        expect(result.current).toEqual([]);
        expect(convertGlossaryTermToTreeNode).not.toHaveBeenCalled();
    });

    it('returns empty array when empty glossary terms array provided', () => {
        const { result } = renderHook(() => useTreeNodesFromGlossaryTerms([]));

        expect(result.current).toEqual([]);
        expect(convertGlossaryTermToTreeNode).not.toHaveBeenCalled();
    });

    it('converts glossary terms to tree nodes', () => {
        const { result } = renderHook(() => useTreeNodesFromGlossaryTerms([mockTerm1, mockTerm2]));

        expect(result.current).toEqual([mockTreeNode1, mockTreeNode2]);
        expect(convertGlossaryTermToTreeNode).toHaveBeenCalledTimes(2);
    });
});
