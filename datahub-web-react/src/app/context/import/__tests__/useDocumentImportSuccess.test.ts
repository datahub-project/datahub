import { act, renderHook } from '@testing-library/react-hooks';
import { describe, expect, it, vi } from 'vitest';

import '@app/context/import/__tests__/testSetup';
import { useDocumentImportSuccess } from '@app/context/import/hooks/useDocumentImportSuccess';

describe('useDocumentImportSuccess', () => {
    it('reloads root children after import', () => {
        vi.useFakeTimers();
        const loadChildren = vi.fn();
        const { result } = renderHook(() =>
            useDocumentImportSuccess({
                loadChildren,
            }),
        );

        act(() => {
            result.current();
        });
        act(() => {
            vi.runAllTimers();
        });

        expect(loadChildren).toHaveBeenCalledWith(null);
        expect(loadChildren).toHaveBeenCalledTimes(3);
        vi.useRealTimers();
    });

    it('also refreshes domain-grouped documents when a callback is provided', () => {
        vi.useFakeTimers();
        const loadChildren = vi.fn();
        const onRefreshGroupedDocuments = vi.fn();
        const { result } = renderHook(() =>
            useDocumentImportSuccess({
                loadChildren,
                onRefreshGroupedDocuments,
            }),
        );

        act(() => {
            result.current();
        });
        act(() => {
            vi.runAllTimers();
        });

        expect(loadChildren).toHaveBeenCalledTimes(3);
        expect(onRefreshGroupedDocuments).toHaveBeenCalledTimes(3);
        vi.useRealTimers();
    });
});
