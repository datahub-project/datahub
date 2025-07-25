import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { toPng } from 'html-to-image';
import React from 'react';
import { getRectOfNodes, getTransformForBounds, useReactFlow } from 'reactflow';

import { LineageNodesContext } from '@app/lineageV3/common';
import DownloadLineageScreenshotButton from '@app/lineageV3/controls/DownloadLineageScreenshotButton';
import { downloadImage } from '@app/lineageV3/utils/lineageUtils';

// Mock external dependencies
vi.mock('html-to-image');
vi.mock('reactflow');
// Mock lineageUtils - we'll handle downloadImage separately in tests
vi.mock('@app/lineageV3/utils/lineageUtils', () => ({
    downloadImage: vi.fn(),
}));

// Mock Ant Design icons partially
vi.mock('@ant-design/icons', async (importOriginal) => {
    const actual = (await importOriginal()) as any;
    return {
        ...actual,
        CameraOutlined: () => <span data-testid="camera-icon">📷</span>,
    };
});

// Mock StyledPanelButton
vi.mock('@app/lineageV3/controls/StyledPanelButton', () => ({
    StyledPanelButton: ({ children, onClick, ...props }: any) => (
        <button type="button" onClick={onClick} {...props}>
            {children}
        </button>
    ),
}));

// Mock context data
const mockNodes = new Map([
    [
        'urn:li:dataset:test',
        {
            entity: { name: 'Test Dataset' },
            urn: 'urn:li:dataset:test',
        },
    ],
    [
        'urn:li:dataset:special-chars',
        {
            entity: { name: 'dataset-with/special@chars#and$symbols' },
            urn: 'urn:li:dataset:special-chars',
        },
    ],
]);

const mockContextValue = {
    rootUrn: 'urn:li:dataset:test',
    rootType: 'dataset' as any,
    nodes: mockNodes,
    edges: new Map(),
    adjacencyList: new Map(),
    nodeVersion: new Map(),
    setNodeVersion: vi.fn(),
    dataVersion: new Map(),
    setDataVersion: vi.fn(),
    displayVersion: new Map(),
    setDisplayVersion: vi.fn(),
    loading: false,
    error: null,
    selectedFields: new Set(),
    hoveredField: null,
    expandedNodes: new Set(),
    filteredNodes: new Set(),
    filteredEdges: new Set(),
    focusedNodes: new Set(),
    hiddenNodes: new Set(),
    hiddenEdges: new Set(),
    ghostNodes: new Set(),
    ghostEdges: new Set(),
    setSelectedFields: vi.fn(),
    setHoveredField: vi.fn(),
    setExpandedNodes: vi.fn(),
    setFocusedNodes: vi.fn(),
} as any;

const mockUseReactFlow = {
    getNodes: vi.fn(() => [
        { id: '1', position: { x: 0, y: 0 }, data: {} },
        { id: '2', position: { x: 100, y: 100 }, data: {} },
    ]),
};

// Mock implementations
beforeEach(() => {
    vi.clearAllMocks();

    (useReactFlow as any).mockReturnValue(mockUseReactFlow);
    (getRectOfNodes as any).mockReturnValue({
        width: 800,
        height: 600,
        x: 0,
        y: 0,
    });
    (getTransformForBounds as any).mockReturnValue([100, 50, 0.75]);
    (toPng as any).mockResolvedValue('data:image/png;base64,mockdata');
    (downloadImage as any).mockImplementation(() => {});

    // Mock document.querySelector
    const mockViewport = document.createElement('div');
    mockViewport.className = 'react-flow__viewport';
    vi.spyOn(document, 'querySelector').mockReturnValue(mockViewport);
});

afterEach(() => {
    vi.restoreAllMocks();
});

describe('DownloadLineageScreenshotButton', () => {
    const renderComponent = (showExpandedText = false, contextValue = mockContextValue) => {
        return render(
            <LineageNodesContext.Provider value={contextValue}>
                <DownloadLineageScreenshotButton showExpandedText={showExpandedText} />
            </LineageNodesContext.Provider>,
        );
    };

    describe('Rendering', () => {
        it('should render button with camera icon', () => {
            renderComponent();

            const button = screen.getByRole('button');
            expect(button).toBeInTheDocument();

            // Check for camera icon
            const icon = screen.getByTestId('camera-icon');
            expect(icon).toBeInTheDocument();
        });

        it('should show text when showExpandedText is true', () => {
            renderComponent(true);

            expect(screen.getByText('Screenshot')).toBeInTheDocument();
        });

        it('should not show text when showExpandedText is false', () => {
            renderComponent(false);

            expect(screen.queryByText('Screenshot')).not.toBeInTheDocument();
        });
    });

    describe('Screenshot functionality', () => {
        it('should call screenshot APIs when button is clicked', async () => {
            renderComponent();

            const button = screen.getByRole('button');
            fireEvent.click(button);

            await waitFor(() => {
                expect(getRectOfNodes).toHaveBeenCalledWith(mockUseReactFlow.getNodes());
                expect(getTransformForBounds).toHaveBeenCalledWith(
                    { width: 800, height: 600, x: 0, y: 0 },
                    1000, // width + 200
                    800, // height + 200
                    0.5,
                    2,
                );
            });
        });

        it('should call toPng with correct parameters', async () => {
            renderComponent();

            const button = screen.getByRole('button');
            fireEvent.click(button);

            await waitFor(() => {
                expect(toPng).toHaveBeenCalledWith(expect.any(HTMLElement), {
                    backgroundColor: '#f8f8f8',
                    width: 1000,
                    height: 800,
                    style: {
                        width: '1000',
                        height: '800',
                        transform: 'translate(100px, 50px) scale(0.75)',
                    },
                });
            });
        });

        it('should call downloadImage with cleaned entity name', async () => {
            renderComponent();

            const button = screen.getByRole('button');
            fireEvent.click(button);

            await waitFor(() => {
                expect(downloadImage).toHaveBeenCalledWith('data:image/png;base64,mockdata', 'Test_Dataset');
            });
        });

        it('should handle entity with special characters in name', async () => {
            const contextWithSpecialChars = {
                ...mockContextValue,
                rootUrn: 'urn:li:dataset:special-chars',
            };

            renderComponent(false, contextWithSpecialChars);

            const button = screen.getByRole('button');
            fireEvent.click(button);

            await waitFor(() => {
                expect(downloadImage).toHaveBeenCalledWith(
                    'data:image/png;base64,mockdata',
                    'dataset-with_special_chars_and_symbols',
                );
            });
        });

        it('should use default lineage name when entity name is not available', async () => {
            const contextWithoutName = {
                ...mockContextValue,
                nodes: new Map([
                    [
                        'urn:li:dataset:test',
                        {
                            entity: { name: '' },
                            urn: 'urn:li:dataset:test',
                        },
                    ],
                ]),
            };

            renderComponent(false, contextWithoutName);

            const button = screen.getByRole('button');
            fireEvent.click(button);

            await waitFor(() => {
                expect(downloadImage).toHaveBeenCalledWith('data:image/png;base64,mockdata', 'lineage');
            });
        });

        it('should handle missing root entity', async () => {
            const contextWithMissingEntity = {
                ...mockContextValue,
                rootUrn: 'urn:li:dataset:nonexistent',
            };

            renderComponent(false, contextWithMissingEntity);

            const button = screen.getByRole('button');
            fireEvent.click(button);

            await waitFor(() => {
                expect(downloadImage).toHaveBeenCalledWith('data:image/png;base64,mockdata', 'lineage');
            });
        });
    });

    describe('Entity name cleaning', () => {
        it('should clean special characters', () => {
            const cleanName = (name: string) => name.replace(/[^a-zA-Z0-9_-]/g, '_');

            expect(cleanName('dataset-with/special@chars#and$symbols')).toBe('dataset-with_special_chars_and_symbols');
            expect(cleanName('user.transactions')).toBe('user_transactions');
            expect(cleanName('normal_name')).toBe('normal_name');
            expect(cleanName('123-valid_name')).toBe('123-valid_name');
        });
    });

    describe('downloadImage', () => {
        let mockAnchorElement: any;
        let originalCreateElement: typeof document.createElement;
        let realDownloadImage: any;

        beforeEach(async () => {
            // Import the real downloadImage function
            const actualModule = (await vi.importActual('@app/lineageV3/utils/lineageUtils')) as any;
            realDownloadImage = actualModule.downloadImage;

            // Mock anchor element
            mockAnchorElement = {
                setAttribute: vi.fn(),
                click: vi.fn(),
            };

            // Mock document.createElement
            originalCreateElement = document.createElement;
            document.createElement = vi.fn().mockReturnValue(mockAnchorElement);
        });

        afterEach(() => {
            document.createElement = originalCreateElement;
        });

        it('should create anchor element and set download attribute with default filename', () => {
            const dataUrl = 'data:image/png;base64,mockdata';

            realDownloadImage(dataUrl);

            expect(document.createElement).toHaveBeenCalledWith('a');
            expect(mockAnchorElement.setAttribute).toHaveBeenCalledWith('href', dataUrl);
            expect(mockAnchorElement.setAttribute).toHaveBeenCalledWith(
                'download',
                expect.stringMatching(/^reactflow_\d{4}-\d{2}-\d{2}_\d{6}\.png$/),
            );
            expect(mockAnchorElement.click).toHaveBeenCalled();
        });

        it('should handle empty string name parameter and use default prefix', () => {
            const dataUrl = 'data:image/png;base64,mockdata';

            realDownloadImage(dataUrl, '');

            expect(mockAnchorElement.setAttribute).toHaveBeenCalledWith(
                'download',
                expect.stringMatching(/^reactflow_\d{4}-\d{2}-\d{2}_\d{6}\.png$/),
            );
        });

        it('should generate filename with correct format and timestamp', () => {
            const dataUrl = 'data:image/png;base64,mockdata';
            const name = 'test_entity';

            realDownloadImage(dataUrl, name);

            // Verify that the anchor element is created and the href attribute is set
            expect(mockAnchorElement.setAttribute).toHaveBeenCalledWith('href', dataUrl);
            expect(mockAnchorElement.click).toHaveBeenCalledTimes(1);

            // Get the download filename from the setAttribute calls
            const setAttributeCalls = mockAnchorElement.setAttribute.mock.calls;
            const downloadCall = setAttributeCalls.find((call: any[]) => call[0] === 'download');
            const filename = downloadCall[1];

            // Verify filename format: test_entity_YYYY-MM-DD_HHMMSS.png
            expect(filename).toMatch(/^test_entity_\d{4}-\d{2}-\d{2}_\d{6}\.png$/);

            // Extract and verify date part (YYYY-MM-DD)
            const parts = filename.split('_');
            const datePart = parts[2];
            expect(datePart).toMatch(/^\d{4}-\d{2}-\d{2}$/);

            // Extract and verify time part (HHMMSS)
            const timePart = parts[3].replace('.png', '');
            expect(timePart).toMatch(/^\d{6}$/);
            expect(timePart.length).toBe(6);
        });
    });
});
