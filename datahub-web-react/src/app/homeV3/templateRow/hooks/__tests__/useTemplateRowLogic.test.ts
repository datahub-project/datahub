import { renderHook } from '@testing-library/react-hooks';
import { vi } from 'vitest';

import { useDragRowContext } from '@app/homeV3/templateRow/hooks/useDragRowContext';
import { useTemplateRowLogic } from '@app/homeV3/templateRow/hooks/useTemplateRowLogic';
import { WrappedRow } from '@app/homeV3/templateRow/types';

import { DataHubPageModuleType, EntityType, PageModuleScope } from '@types';

// Mock the useDragRowContext hook
vi.mock('@app/homeV3/templateRow/hooks/useDragRowContext', () => ({
    useDragRowContext: vi.fn(),
}));
const mockUseDragRowContext = vi.mocked(useDragRowContext);

describe('useTemplateRowLogic', () => {
    const createMockRow = (moduleCount: number): WrappedRow => ({
        modules: Array.from({ length: moduleCount }).map((_, i) => ({
            urn: `urn:li:module:${i}`,
            type: EntityType.DatahubPageModule,
            properties: {
                name: `Module ${i}`,
                type: DataHubPageModuleType.OwnedAssets,
                visibility: { scope: PageModuleScope.Personal },
                params: {},
            },
        })),
        originRowIndex: 0,
        rowIndex: 0,
    });

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('row constraints and state', () => {
        it('should correctly identify when row is full (3 modules)', () => {
            const row = createMockRow(3);
            mockUseDragRowContext.mockReturnValue(false);

            const { result } = renderHook(() => useTemplateRowLogic(row, 0));

            expect(result.current.isRowFull).toBe(true);
            expect(result.current.currentModuleCount).toBe(3);
            expect(result.current.maxModulesPerRow).toBe(3);
        });

        it('should correctly identify when row is not full', () => {
            const row = createMockRow(2);
            mockUseDragRowContext.mockReturnValue(false);

            const { result } = renderHook(() => useTemplateRowLogic(row, 0));

            expect(result.current.isRowFull).toBe(false);
            expect(result.current.currentModuleCount).toBe(2);
        });

        it('should handle empty row', () => {
            const row = createMockRow(0);
            mockUseDragRowContext.mockReturnValue(false);

            const { result } = renderHook(() => useTemplateRowLogic(row, 0));

            expect(result.current.isRowFull).toBe(false);
            expect(result.current.currentModuleCount).toBe(0);
            expect(result.current.modulePositions).toEqual([]);
        });
    });

    describe('drag state logic', () => {
        it('should disable drop zones when row is full and dragging from different row', () => {
            const row = createMockRow(3);
            mockUseDragRowContext.mockReturnValue(false); // Dragging from different row

            const { result } = renderHook(() => useTemplateRowLogic(row, 0));

            expect(result.current.shouldDisableDropZones).toBe(true);
            expect(result.current.isDraggingFromSameRow).toBe(false);
        });

        it('should not disable drop zones when row is full but dragging from same row', () => {
            const row = createMockRow(3);
            mockUseDragRowContext.mockReturnValue(true); // Dragging from same row

            const { result } = renderHook(() => useTemplateRowLogic(row, 0));

            expect(result.current.shouldDisableDropZones).toBe(false);
            expect(result.current.isDraggingFromSameRow).toBe(true);
        });

        it('should not disable drop zones when row is not full', () => {
            const row = createMockRow(2);
            mockUseDragRowContext.mockReturnValue(false);

            const { result } = renderHook(() => useTemplateRowLogic(row, 0));

            expect(result.current.shouldDisableDropZones).toBe(false);
        });
    });

    describe('module positions generation', () => {
        it('should generate correct positions for multiple modules', () => {
            const row = createMockRow(2);
            mockUseDragRowContext.mockReturnValue(false);

            const { result } = renderHook(() => useTemplateRowLogic(row, 1));

            expect(result.current.modulePositions).toHaveLength(2);

            // First module (index 0) should be on left
            expect(result.current.modulePositions[0]).toEqual({
                module: row.modules[0],
                position: {
                    rowIndex: 1,
                    moduleIndex: 0,
                    rowSide: 'left',
                },
                key: 'urn:li:module:0-0',
            });

            // Second module (index 1) should be on right
            expect(result.current.modulePositions[1]).toEqual({
                module: row.modules[1],
                position: {
                    rowIndex: 1,
                    moduleIndex: 1,
                    rowSide: 'right',
                },
                key: 'urn:li:module:1-1',
            });
        });

        it('should generate unique keys for each module', () => {
            const row = createMockRow(3);
            mockUseDragRowContext.mockReturnValue(false);

            const { result } = renderHook(() => useTemplateRowLogic(row, 2));

            const keys = result.current.modulePositions.map((pos) => pos.key);
            expect(keys).toEqual(['urn:li:module:0-0', 'urn:li:module:1-1', 'urn:li:module:2-2']);
            expect(new Set(keys).size).toBe(3); // All keys are unique
        });

        it('should memoize module positions when inputs dont change', () => {
            const row = createMockRow(2);
            mockUseDragRowContext.mockReturnValue(false);

            const { result, rerender } = renderHook(() => useTemplateRowLogic(row, 0));

            const firstRender = result.current.modulePositions;

            // Re-render with same inputs
            rerender();

            const secondRender = result.current.modulePositions;

            // Should be the same reference (memoized)
            expect(firstRender).toBe(secondRender);
        });
    });

    describe('edge cases', () => {
        it('should handle single module correctly', () => {
            const row = createMockRow(1);
            mockUseDragRowContext.mockReturnValue(false);

            const { result } = renderHook(() => useTemplateRowLogic(row, 0));

            expect(result.current.isRowFull).toBe(false);
            expect(result.current.modulePositions).toHaveLength(1);
            expect(result.current.modulePositions[0].position.rowSide).toBe('left');
        });

        it('should update when row index changes', () => {
            const row = createMockRow(1);
            mockUseDragRowContext.mockReturnValue(false);

            const { result, rerender } = renderHook(({ rowIndex }) => useTemplateRowLogic(row, rowIndex), {
                initialProps: { rowIndex: 0 },
            });

            expect(result.current.modulePositions[0].position.rowIndex).toBe(0);

            // Change row index
            rerender({ rowIndex: 2 });

            expect(result.current.modulePositions[0].position.rowIndex).toBe(2);
        });
    });
});
