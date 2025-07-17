import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useGlossaryNodesAndTerms';
import useInitialGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useInitialGlossaryNodesAndTerms';

import { EntityType, GlossaryNode, GlossaryTerm } from '@types';

// Mock the dependent hook
vi.mock(
    '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useGlossaryNodesAndTerms',
);

describe('useInitialGlossaryNodesAndTerms', () => {
    const mockNodes: GlossaryNode[] = [
        { urn: 'node1', type: EntityType.GlossaryNode },
        { urn: 'node2', type: EntityType.GlossaryNode },
    ];

    const mockTerms: GlossaryTerm[] = [
        { urn: 'term1', name: 'Term 1', hierarchicalName: 'Term 1', type: EntityType.GlossaryTerm },
        { urn: 'term2', name: 'Term 2', hierarchicalName: 'Term 2', type: EntityType.GlossaryTerm },
    ];

    beforeEach(() => {
        vi.clearAllMocks();
        vi.mocked(useGlossaryNodesAndTerms).mockReturnValue({
            glossaryNodes: undefined,
            glossaryTerms: undefined,
            entities: undefined,
            data: undefined,
            loading: false,
        });
    });

    it('passes correct URNs to useGlossaryNodesAndTerms', () => {
        const urns = ['urn1', 'urn2'];
        renderHook(() => useInitialGlossaryNodesAndTerms(urns));

        expect(useGlossaryNodesAndTerms).toHaveBeenCalledWith({
            glossaryNodesAndTermsUrns: urns,
        });
    });

    it('returns undefined when inner hook returns undefined', () => {
        vi.mocked(useGlossaryNodesAndTerms).mockReturnValue({
            glossaryNodes: undefined,
            glossaryTerms: undefined,
            entities: undefined,
            data: undefined,
            loading: true,
        });

        const { result } = renderHook(() => useInitialGlossaryNodesAndTerms(['urn1']));

        expect(result.current).toEqual({
            glossaryNodes: undefined,
            glossaryTerms: undefined,
        });
    });

    it('returns nodes and terms from inner hook', () => {
        vi.mocked(useGlossaryNodesAndTerms).mockReturnValue({
            glossaryNodes: mockNodes,
            glossaryTerms: mockTerms,
            entities: [...mockNodes, ...mockTerms],
            data: {},
            loading: false,
        });

        const { result } = renderHook(() => useInitialGlossaryNodesAndTerms(['urn1']));

        expect(result.current).toEqual({
            glossaryNodes: mockNodes,
            glossaryTerms: mockTerms,
        });
    });

    it('returns empty arrays when inner hook returns empty arrays', () => {
        vi.mocked(useGlossaryNodesAndTerms).mockReturnValue({
            glossaryNodes: [],
            glossaryTerms: [],
            entities: [],
            data: {},
            loading: false,
        });

        const { result } = renderHook(() => useInitialGlossaryNodesAndTerms(['urn1']));

        expect(result.current).toEqual({
            glossaryNodes: [],
            glossaryTerms: [],
        });
    });

    it('handles partial data', () => {
        vi.mocked(useGlossaryNodesAndTerms).mockReturnValue({
            glossaryNodes: mockNodes,
            glossaryTerms: [],
            entities: mockNodes,
            data: {},
            loading: false,
        });

        const { result } = renderHook(() => useInitialGlossaryNodesAndTerms(['urn1']));

        expect(result.current).toEqual({
            glossaryNodes: mockNodes,
            glossaryTerms: [],
        });
    });

    it('does not pass parentGlossaryNodeUrn', () => {
        renderHook(() => useInitialGlossaryNodesAndTerms(['urn1']));

        const callProps = vi.mocked(useGlossaryNodesAndTerms).mock.calls[0][0];
        expect(callProps).not.toHaveProperty('parentGlossaryNodeUrn');
    });

    it('handles empty URNs array', () => {
        renderHook(() => useInitialGlossaryNodesAndTerms([]));

        expect(useGlossaryNodesAndTerms).toHaveBeenCalledWith({
            glossaryNodesAndTermsUrns: [],
        });
    });
});
