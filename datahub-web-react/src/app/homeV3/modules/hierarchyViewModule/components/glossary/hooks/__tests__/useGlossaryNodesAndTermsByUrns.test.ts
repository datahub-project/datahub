import { renderHook } from '@testing-library/react-hooks';

import { useModuleContext } from '@app/homeV3/module/context/ModuleContext';
import useGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useGlossaryNodesAndTerms';
import useGlossaryNodesAndTermsByUrns from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useGlossaryNodesAndTermsByUrns';

vi.mock('@app/homeV3/module/context/ModuleContext', () => ({
    useModuleContext: vi.fn(),
}));
vi.mock('@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useGlossaryNodesAndTerms', () => ({
    __esModule: true,
    default: vi.fn(),
}));

describe('useGlossaryNodesAndTermsByUrns', () => {
    const mockGlossaryNodesAndTerms = [
        { urn: 'urn:li:glossaryNode:node1', name: 'Node 1' },
        { urn: 'urn:li:glossaryTerm:term1', name: 'Term 1' },
    ];
    const mockOnReloadingFinished = vi.fn();

    beforeEach(() => {
        (useModuleContext as unknown as any).mockReturnValue({
            isReloading: false,
            onReloadingFinished: mockOnReloadingFinished,
        });
        (useGlossaryNodesAndTerms as unknown as any).mockReturnValue({
            loading: false,
            entities: mockGlossaryNodesAndTerms,
            glossaryNodes: [mockGlossaryNodesAndTerms[0]],
            glossaryTerms: [mockGlossaryNodesAndTerms[1]],
            total: mockGlossaryNodesAndTerms.length,
        });
    });

    afterEach(() => {
        vi.resetAllMocks();
    });

    it('should call useGlossaryNodesAndTerms with correct props', () => {
        const urns = ['urn:li:glossaryNode:node1', 'urn:li:glossaryTerm:term1'];
        renderHook(() => useGlossaryNodesAndTermsByUrns(urns));

        expect(useGlossaryNodesAndTerms).toHaveBeenCalledWith(
            expect.objectContaining({
                glossaryNodesAndTermsUrns: urns,
                count: urns.length,
                fetchPolicy: 'cache-first',
                onCompleted: expect.any(Function),
            }),
        );
    });

    it('should set fetchPolicy to cache-and-network when isReloading is true', () => {
        (useModuleContext as unknown as any).mockReturnValueOnce({
            isReloading: true,
            onReloadingFinished: mockOnReloadingFinished,
        });
        const urns = ['urn:li:glossaryNode:node1'];
        renderHook(() => useGlossaryNodesAndTermsByUrns(urns));

        expect(useGlossaryNodesAndTerms).toHaveBeenCalledWith(
            expect.objectContaining({
                fetchPolicy: 'cache-and-network',
            }),
        );
    });

    it('should call onReloadingFinished when onCompleted is triggered', () => {
        const urns = ['urn:li:glossaryNode:node1'];
        let onCompletedCallback: () => void = () => {};
        (useGlossaryNodesAndTerms as unknown as any).mockImplementationOnce(({ onCompleted }) => {
            onCompletedCallback = onCompleted;
            return {
                loading: false,
                entities: mockGlossaryNodesAndTerms,
                glossaryNodes: [mockGlossaryNodesAndTerms[0]],
                glossaryTerms: [mockGlossaryNodesAndTerms[1]],
                total: mockGlossaryNodesAndTerms.length,
            };
        });

        renderHook(() => useGlossaryNodesAndTermsByUrns(urns));

        // Manually trigger the onCompleted callback
        (onCompletedCallback as any)();

        expect(mockOnReloadingFinished).toHaveBeenCalledTimes(1);
    });

    it('should return the results from useGlossaryNodesAndTerms', () => {
        const urns = ['urn:li:glossaryNode:node1'];
        const { result } = renderHook(() => useGlossaryNodesAndTermsByUrns(urns));

        expect(result.current.loading).toBe(false);
        expect(result.current.entities).toEqual(mockGlossaryNodesAndTerms);
        expect(result.current.glossaryNodes).toEqual([mockGlossaryNodesAndTerms[0]]);
        expect(result.current.glossaryTerms).toEqual([mockGlossaryNodesAndTerms[1]]);
        expect(result.current.total).toBe(mockGlossaryNodesAndTerms.length);
    });
});
