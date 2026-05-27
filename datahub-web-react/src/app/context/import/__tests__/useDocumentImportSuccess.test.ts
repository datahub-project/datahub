import { act, renderHook } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { useDocumentImportSuccess } from '@app/context/import/hooks/useDocumentImportSuccess';

describe('useDocumentImportSuccess', () => {
    it('reloads root children when parent is null', () => {
        vi.useFakeTimers();
        const loadChildren = vi.fn();
        const { result } = renderHook(() =>
            useDocumentImportSuccess({
                expandNode: vi.fn(),
                getNode: vi.fn(),
                loadChildren,
            }),
        );

        act(() => {
            result.current(null);
        });
        act(() => {
            vi.runAllTimers();
        });

        expect(loadChildren).toHaveBeenCalledWith(null);
        vi.useRealTimers();
    });

    it('expands ancestors and reloads parent subtree when parent is set', () => {
        vi.useFakeTimers();
        const expandNode = vi.fn();
        const loadChildren = vi.fn();
        const getNode = vi.fn((urn: string) => {
            if (urn === 'urn:li:document:child') {
                return { urn, parentUrn: 'urn:li:document:parent' };
            }
            if (urn === 'urn:li:document:parent') {
                return { urn, parentUrn: undefined };
            }
            return undefined;
        });

        const { result } = renderHook(() =>
            useDocumentImportSuccess({
                expandNode,
                getNode,
                loadChildren,
            }),
        );

        act(() => {
            result.current('urn:li:document:child');
        });
        act(() => {
            vi.runAllTimers();
        });

        expect(expandNode).toHaveBeenCalledWith('urn:li:document:child');
        expect(expandNode).toHaveBeenCalledWith('urn:li:document:parent');
        expect(loadChildren).toHaveBeenCalledWith('urn:li:document:child');
        vi.useRealTimers();
    });
});
