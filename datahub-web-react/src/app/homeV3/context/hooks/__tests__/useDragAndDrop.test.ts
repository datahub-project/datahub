import { act, renderHook } from '@testing-library/react-hooks';
import { vi } from 'vitest';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import { useDragAndDrop } from '@app/homeV3/context/hooks/useDragAndDrop';

import { DataHubPageModuleType, EntityType, PageModuleScope } from '@types';

// Mock the PageTemplateContext
vi.mock('@app/homeV3/context/PageTemplateContext', () => ({
    usePageTemplateContext: vi.fn(),
}));

const mockUsePageTemplateContext = vi.mocked(usePageTemplateContext);

// Mock template data
const mockModule1 = {
    urn: 'urn:li:pageModule:1',
    type: EntityType.DatahubPageModule,
    properties: {
        name: 'Module 1',
        type: DataHubPageModuleType.OwnedAssets,
        visibility: { scope: PageModuleScope.Personal },
        params: {},
    },
};

const mockModule2 = {
    urn: 'urn:li:pageModule:2',
    type: EntityType.DatahubPageModule,
    properties: {
        name: 'Module 2',
        type: DataHubPageModuleType.Domains,
        visibility: { scope: PageModuleScope.Personal },
        params: {},
    },
};

const mockTemplate = {
    urn: 'urn:li:pageTemplate:test',
    type: EntityType.DatahubPageTemplate,
    properties: {
        rows: [
            {
                modules: [mockModule1, mockModule2],
            },
        ],
    },
};

describe('useDragAndDrop', () => {
    const mockMoveModule = vi.fn();

    beforeEach(() => {
        vi.clearAllMocks();

        mockUsePageTemplateContext.mockReturnValue({
            moveModule: mockMoveModule,
        } as any);
    });

    describe('activeModule state management', () => {
        it('should initialize with null activeModule', () => {
            const { result } = renderHook(() => useDragAndDrop());
            
            expect(result.current.activeModule).toBeNull();
        });

        it('should set activeModule on drag start', () => {
            const { result } = renderHook(() => useDragAndDrop());

            const mockDragStartEvent = {
                active: {
                    data: {
                        current: {
                            module: mockModule1,
                            position: { rowIndex: 0, moduleIndex: 0 },
                        },
                    },
                },
            };

            act(() => {
                result.current.handleDragStart(mockDragStartEvent as any);
            });

            expect(result.current.activeModule).toEqual({
                module: mockModule1,
                position: { rowIndex: 0, moduleIndex: 0 },
            });
        });

        it('should not set activeModule when drag data is missing', () => {
            const { result } = renderHook(() => useDragAndDrop());

            const mockDragStartEvent = {
                active: {
                    data: {
                        current: null,
                    },
                },
            };

            act(() => {
                result.current.handleDragStart(mockDragStartEvent as any);
            });

            expect(result.current.activeModule).toBeNull();
        });

        it('should clear activeModule on drag end', async () => {
            const { result } = renderHook(() => useDragAndDrop());

            // First set activeModule
            const mockDragStartEvent = {
                active: {
                    data: {
                        current: {
                            module: mockModule1,
                            position: { rowIndex: 0, moduleIndex: 0 },
                        },
                    },
                },
            };

            act(() => {
                result.current.handleDragStart(mockDragStartEvent as any);
            });

            expect(result.current.activeModule).not.toBeNull();

            // Then clear it on drag end
            const mockDragEndEvent = {
                active: {
                    data: {
                        current: {
                            module: mockModule1,
                            position: { rowIndex: 0, moduleIndex: 0 },
                        },
                    },
                },
                over: {
                    data: {
                        current: {
                            rowIndex: 0,
                            moduleIndex: 1,
                        },
                    },
                },
            };

            await act(async () => {
                await result.current.handleDragEnd(mockDragEndEvent as any);
            });

            expect(result.current.activeModule).toBeNull();
        });
    });

    describe('insertNewRow functionality', () => {
        it('should pass insertNewRow flag to moveModule', async () => {
            const { result } = renderHook(() => useDragAndDrop());

            const mockDragEndEvent = {
                active: {
                    data: {
                        current: {
                            module: mockModule1,
                            position: { rowIndex: 0, moduleIndex: 0 },
                        },
                    },
                },
                over: {
                    data: {
                        current: {
                            rowIndex: 1,
                            moduleIndex: 0,
                            insertNewRow: true,
                        },
                    },
                },
            };

            await act(async () => {
                await result.current.handleDragEnd(mockDragEndEvent as any);
            });

            expect(mockMoveModule).toHaveBeenCalledWith({
                module: mockModule1,
                fromPosition: { rowIndex: 0, moduleIndex: 0 },
                toPosition: {
                    rowIndex: 1,
                    moduleIndex: 0,
                    rowSide: 'left',
                },
                insertNewRow: true,
            });
        });
    });

    it('should call moveModule with correct parameters on drag end', async () => {
        const { result } = renderHook(() => useDragAndDrop());

        const mockDragEndEvent = {
            active: {
                data: {
                    current: {
                        module: mockModule1,
                        position: { rowIndex: 0, moduleIndex: 0 },
                    },
                },
            },
            over: {
                data: {
                    current: {
                        rowIndex: 0,
                        moduleIndex: 1,
                    },
                },
            },
        };

        await act(async () => {
            await result.current.handleDragEnd(mockDragEndEvent as any);
        });

        expect(mockMoveModule).toHaveBeenCalledWith({
            module: mockModule1,
            fromPosition: { rowIndex: 0, moduleIndex: 0 },
            toPosition: {
                rowIndex: 0,
                moduleIndex: 1,
                rowSide: 'right',
            },
            insertNewRow: undefined,
        });
    });

    it('should not call moveModule when dropping in the same position', async () => {
        const { result } = renderHook(() => useDragAndDrop());

        const mockDragEndEvent = {
            active: {
                data: {
                    current: {
                        module: mockModule1,
                        position: { rowIndex: 0, moduleIndex: 0 },
                    },
                },
            },
            over: {
                data: {
                    current: {
                        rowIndex: 0,
                        moduleIndex: 0,
                    },
                },
            },
        };

        await act(async () => {
            await result.current.handleDragEnd(mockDragEndEvent as any);
        });

        expect(mockMoveModule).not.toHaveBeenCalled();
    });

    it('should not call moveModule when drag or drop data is missing', async () => {
        const { result } = renderHook(() => useDragAndDrop());

        const mockDragEndEvent = {
            active: {
                data: null,
            },
            over: {
                data: {
                    current: {
                        rowIndex: 0,
                        moduleIndex: 1,
                    },
                },
            },
        };

        await act(async () => {
            await result.current.handleDragEnd(mockDragEndEvent as any);
        });

        expect(mockMoveModule).not.toHaveBeenCalled();
    });

    it('should set correct rowSide based on module index', async () => {
        const { result } = renderHook(() => useDragAndDrop());

        // Test dropping at index 0 (should be 'left')
        const mockDragEndEventLeft = {
            active: {
                data: {
                    current: {
                        module: mockModule1,
                        position: { rowIndex: 0, moduleIndex: 1 },
                    },
                },
            },
            over: {
                data: {
                    current: {
                        rowIndex: 1,
                        moduleIndex: 0,
                    },
                },
            },
        };

        await act(async () => {
            await result.current.handleDragEnd(mockDragEndEventLeft as any);
        });

        expect(mockMoveModule).toHaveBeenCalledWith({
            module: mockModule1,
            fromPosition: { rowIndex: 0, moduleIndex: 1 },
            toPosition: {
                rowIndex: 1,
                moduleIndex: 0,
                rowSide: 'left',
            },
            insertNewRow: undefined,
        });
    });
});
