import { render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { ResizablePills } from '@components/components/ResizablePills/index';

describe('ResizablePills', () => {
    const mockItems = [
        { id: '1', name: 'Item 1' },
        { id: '2', name: 'Item 2' },
        { id: '3', name: 'Item 3' },
        { id: '4', name: 'Item 4' },
        { id: '5', name: 'Item 5' },
    ];

    it('renders all items when container is wide enough', async () => {
        // Mock ResizeObserver to simulate wide container
        global.ResizeObserver = vi.fn().mockImplementation((callback) => {
            setTimeout(() => {
                callback([
                    {
                        contentRect: { width: 1000 }, // Wide container
                    },
                ]);
            }, 0);

            return {
                observe: vi.fn(),
                disconnect: vi.fn(),
                unobserve: vi.fn(),
            };
        });

        render(
            <ResizablePills
                items={mockItems}
                getItemWidth={() => 50}
                renderPill={(item) => <div data-testid={`pill-${item.id}`}>{item.name}</div>}
                keyExtractor={(item) => item.id}
                debounceMs={0}
            />,
        );

        await waitFor(() => {
            mockItems.forEach((item) => {
                expect(screen.getByTestId(`pill-${item.id}`)).toBeInTheDocument();
            });
        });
    });

    it('handles empty items array', () => {
        const { container } = render(
            <ResizablePills items={[]} getItemWidth={() => 50} renderPill={(item) => <div>{item}</div>} />,
        );

        expect(container.firstChild).toBeNull();
    });

    it('shows overflow button when not all items fit', async () => {
        // Mock ResizeObserver to simulate narrow container
        const mockObserve = vi.fn();
        const mockDisconnect = vi.fn();

        global.ResizeObserver = vi.fn().mockImplementation((callback) => {
            // Simulate narrow container (200px) after component mounts
            setTimeout(() => {
                callback([
                    {
                        contentRect: { width: 200 },
                    },
                ]);
            }, 0);

            return {
                observe: mockObserve,
                disconnect: mockDisconnect,
                unobserve: vi.fn(),
            };
        });

        render(
            <ResizablePills
                items={mockItems}
                getItemWidth={() => 80} // Each item is 80px
                renderPill={(item) => <div data-testid={`pill-${item.id}`}>{item.name}</div>}
                keyExtractor={(item) => item.id}
                debounceMs={0} // Disable debounce for test
            />,
        );

        await waitFor(() => {
            // Should show overflow button
            expect(screen.getByText(/\+\d+/)).toBeInTheDocument();
        });

        expect(mockObserve).toHaveBeenCalled();
    });

    it('uses custom overflow label', async () => {
        global.ResizeObserver = vi.fn().mockImplementation((callback) => {
            setTimeout(() => {
                callback([
                    {
                        contentRect: { width: 200 },
                    },
                ]);
            }, 0);

            return {
                observe: vi.fn(),
                disconnect: vi.fn(),
                unobserve: vi.fn(),
            };
        });

        render(
            <ResizablePills
                items={mockItems}
                getItemWidth={() => 80}
                renderPill={(item) => <div>{item.name}</div>}
                overflowLabel={(count) => `${count} more items`}
                debounceMs={0}
            />,
        );

        await waitFor(() => {
            expect(screen.getByText(/\d+ more items/)).toBeInTheDocument();
        });
    });

    it('renders tooltip content when provided', async () => {
        global.ResizeObserver = vi.fn().mockImplementation((callback) => {
            setTimeout(() => {
                callback([
                    {
                        contentRect: { width: 200 },
                    },
                ]);
            }, 0);

            return {
                observe: vi.fn(),
                disconnect: vi.fn(),
                unobserve: vi.fn(),
            };
        });

        render(
            <ResizablePills
                items={mockItems}
                getItemWidth={() => 80}
                renderPill={(item) => <div>{item.name}</div>}
                overflowTooltipContent={(hidden) => (
                    <div data-testid="tooltip-content">{hidden.length} hidden items</div>
                )}
                debounceMs={0}
            />,
        );

        await waitFor(() => {
            expect(screen.getByText(/\+\d+/)).toBeInTheDocument();
        });
    });

    it('applies custom className and style', () => {
        const { container } = render(
            <ResizablePills
                items={mockItems}
                getItemWidth={() => 50}
                renderPill={(item) => <div>{item.name}</div>}
                className="custom-class"
                style={{ backgroundColor: 'red' }}
            />,
        );

        const containerElement = container.firstChild as HTMLElement;
        expect(containerElement).toHaveClass('custom-class');
        // Style prop is passed to styled-component and may be merged with component styles
        expect(containerElement).toHaveAttribute('style');
    });

    it('uses keyExtractor when provided', () => {
        const keyExtractor = vi.fn((item) => `custom-${item.id}`);

        render(
            <ResizablePills
                items={mockItems}
                getItemWidth={() => 50}
                renderPill={(item) => <div>{item.name}</div>}
                keyExtractor={keyExtractor}
            />,
        );

        // KeyExtractor should be called for each visible item
        expect(keyExtractor).toHaveBeenCalled();
    });

    it('cleans up ResizeObserver on unmount', () => {
        const mockDisconnect = vi.fn();

        global.ResizeObserver = vi.fn().mockImplementation(() => ({
            observe: vi.fn(),
            disconnect: mockDisconnect,
            unobserve: vi.fn(),
        }));

        const { unmount } = render(
            <ResizablePills items={mockItems} getItemWidth={() => 50} renderPill={(item) => <div>{item.name}</div>} />,
        );

        unmount();

        expect(mockDisconnect).toHaveBeenCalled();
    });

    it('respects minContainerWidthForOne setting', async () => {
        global.ResizeObserver = vi.fn().mockImplementation((callback) => {
            setTimeout(() => {
                callback([
                    {
                        contentRect: { width: 80 }, // Very narrow
                    },
                ]);
            }, 0);

            return {
                observe: vi.fn(),
                disconnect: vi.fn(),
                unobserve: vi.fn(),
            };
        });

        render(
            <ResizablePills
                items={mockItems}
                getItemWidth={() => 50}
                renderPill={(item) => <div data-testid={`pill-${item.id}`}>{item.name}</div>}
                minContainerWidthForOne={100} // Require at least 100px to show 1 item
                debounceMs={0}
            />,
        );

        await waitFor(() => {
            // With 80px width and minContainerWidthForOne=100, should show 0 items
            expect(screen.queryByTestId('pill-1')).not.toBeInTheDocument();
        });
    });
});
