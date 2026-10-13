import { AppWindow } from '@phosphor-icons/react/dist/csr/AppWindow';
import { act, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { getLazyIcon } from '@app/mfeframework/lazyIconRegistry';

const MockIcon = () => <div data-testid="mock-icon" />;

// Hoist so vi.mock factory can close over the function reference.
const { loadIconMock } = vi.hoisted(() => ({ loadIconMock: vi.fn() }));

vi.mock('../iconLoader', () => ({ loadIcon: loadIconMock }));

describe('getLazyIcon', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        loadIconMock.mockImplementation(async (name: string) => {
            if (name === 'Known') return { default: MockIcon };
            return { default: AppWindow };
        });
    });

    it('returns a valid React element', () => {
        expect(React.isValidElement(getLazyIcon('Known'))).toBe(true);
    });

    it('does not call loadIcon until the component is rendered — confirms unbundled lazy load', async () => {
        // Calling getLazyIcon only creates the lazy wrapper; no icon fetch yet.
        getLazyIcon('Unfetched');
        expect(loadIconMock).not.toHaveBeenCalled();

        // Rendering is what triggers the dynamic import.
        await act(async () => {
            render(getLazyIcon('Unfetched'));
        });
        await waitFor(() => expect(loadIconMock).toHaveBeenCalledWith('Unfetched'));
    });

    it('renders the correct icon component after Suspense resolves', async () => {
        await act(async () => {
            render(getLazyIcon('Known'));
        });
        await waitFor(() => screen.getByTestId('mock-icon'));
        expect(screen.getByTestId('mock-icon')).toBeInTheDocument();
    });

    it('renders an unknown icon without throwing', async () => {
        await act(async () => {
            render(getLazyIcon('NoSuchIcon'));
        });
        // No crash; loadIcon may resolve to AppWindow for unknown names.
        expect(React.isValidElement(getLazyIcon('NoSuchIcon'))).toBe(true);
    });

    it('caches the lazy component — same React.lazy instance for the same icon name', () => {
        // Two calls with the same name must return elements backed by the same lazy component
        // reference; a new component would reset its resolved state and re-fetch.
        const el1 = getLazyIcon('CacheCheck');
        const el2 = getLazyIcon('CacheCheck');
        const inner1 = (el1 as React.ReactElement).props.children as React.ReactElement;
        const inner2 = (el2 as React.ReactElement).props.children as React.ReactElement;
        expect(inner1.type).toBe(inner2.type);
    });

    it('forwards size/color/weight props to the resolved icon and sizes the Suspense fallback', async () => {
        const PropsCapture = (props: { size?: number; color?: string; weight?: string }) => (
            <div data-testid="props-icon" data-size={props.size} data-color={props.color} data-weight={props.weight} />
        );
        loadIconMock.mockImplementation(async () => ({ default: PropsCapture }));

        await act(async () => {
            render(getLazyIcon('PropsIcon', { size: 28, color: '#123456', weight: 'regular' }));
        });
        await waitFor(() => screen.getByTestId('props-icon'));
        const el = screen.getByTestId('props-icon');
        expect(el).toHaveAttribute('data-size', '28');
        expect(el).toHaveAttribute('data-color', '#123456');
        expect(el).toHaveAttribute('data-weight', 'regular');

        // Fallback sizing: string sizes that coerce to numbers should not collapse to 16.
        const tree = getLazyIcon('PropsIcon', { size: '24' }) as React.ReactElement;
        const { fallback } = tree.props as { fallback: React.ReactElement };
        expect(fallback.props.size).toBe('24');
    });
});
