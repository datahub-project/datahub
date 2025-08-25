import { useDndContext } from '@dnd-kit/core';
import { renderHook } from '@testing-library/react-hooks';
import { vi } from 'vitest';

import { useDragRowContext } from '@app/homeV3/templateRow/hooks/useDragRowContext';

// Mock @dnd-kit/core
vi.mock('@dnd-kit/core', () => ({
    useDndContext: vi.fn(),
}));
const mockUseDndContext = vi.mocked(useDndContext);

describe('useDragRowContext', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should return false when no active drag operation', () => {
        mockUseDndContext.mockReturnValue({
            active: null,
        } as any);

        const { result } = renderHook(() => useDragRowContext(0));

        expect(result.current).toBe(false);
    });

    it('should return false when active drag has no data', () => {
        mockUseDndContext.mockReturnValue({
            active: {
                data: null,
            },
        } as any);

        const { result } = renderHook(() => useDragRowContext(0));

        expect(result.current).toBe(false);
    });

    it('should return false when active drag has no position data', () => {
        mockUseDndContext.mockReturnValue({
            active: {
                data: {
                    current: {
                        // No position property
                    },
                },
            },
        } as any);

        const { result } = renderHook(() => useDragRowContext(0));

        expect(result.current).toBe(false);
    });

    it('should return true when dragging from same row', () => {
        mockUseDndContext.mockReturnValue({
            active: {
                data: {
                    current: {
                        position: {
                            rowIndex: 1,
                            moduleIndex: 0,
                        },
                    },
                },
            },
        } as any);

        const { result } = renderHook(() => useDragRowContext(1));

        expect(result.current).toBe(true);
    });

    it('should return false when dragging from different row', () => {
        mockUseDndContext.mockReturnValue({
            active: {
                data: {
                    current: {
                        position: {
                            rowIndex: 0,
                            moduleIndex: 1,
                        },
                    },
                },
            },
        } as any);

        const { result } = renderHook(() => useDragRowContext(2));

        expect(result.current).toBe(false);
    });

    it('should update when drag context changes', () => {
        // Start with drag from row 0
        mockUseDndContext.mockReturnValue({
            active: {
                data: {
                    current: {
                        position: {
                            rowIndex: 0,
                            moduleIndex: 0,
                        },
                    },
                },
            },
        } as any);

        const { result, rerender } = renderHook(() => useDragRowContext(0));

        expect(result.current).toBe(true);

        // Change to drag from row 1
        mockUseDndContext.mockReturnValue({
            active: {
                data: {
                    current: {
                        position: {
                            rowIndex: 1,
                            moduleIndex: 0,
                        },
                    },
                },
            },
        } as any);

        rerender();

        expect(result.current).toBe(false);
    });

    it('should update when target row index changes', () => {
        mockUseDndContext.mockReturnValue({
            active: {
                data: {
                    current: {
                        position: {
                            rowIndex: 1,
                            moduleIndex: 0,
                        },
                    },
                },
            },
        } as any);

        const { result, rerender } = renderHook(({ rowIndex }) => useDragRowContext(rowIndex), {
            initialProps: { rowIndex: 0 },
        });

        expect(result.current).toBe(false);

        // Change target row to match drag source
        rerender({ rowIndex: 1 });

        expect(result.current).toBe(true);
    });

    it('should memoize result when inputs dont change', () => {
        mockUseDndContext.mockReturnValue({
            active: {
                data: {
                    current: {
                        position: {
                            rowIndex: 0,
                            moduleIndex: 0,
                        },
                    },
                },
            },
        } as any);

        const { result, rerender } = renderHook(() => useDragRowContext(0));

        const firstResult = result.current;

        // Re-render with same mock data
        rerender();

        const secondResult = result.current;

        // Values should be the same
        expect(firstResult).toBe(secondResult);
        expect(firstResult).toBe(true);
    });

    it('should handle edge case with undefined rowIndex in position', () => {
        mockUseDndContext.mockReturnValue({
            active: {
                data: {
                    current: {
                        position: {
                            rowIndex: undefined,
                            moduleIndex: 0,
                        },
                    },
                },
            },
        } as any);

        const { result } = renderHook(() => useDragRowContext(0));

        expect(result.current).toBe(false);
    });
});
