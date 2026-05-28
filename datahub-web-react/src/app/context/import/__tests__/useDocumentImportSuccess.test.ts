import { act, renderHook } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

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
});
