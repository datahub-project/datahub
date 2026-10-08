import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useUserContext } from '@app/context/useUserContext';
import useIsDefaultViewReady, { isDefaultViewReady } from '@app/searchV2/useIsDefaultViewReady';

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: vi.fn(),
}));

const useUserContextMock = vi.mocked(useUserContext);

function mockView(hasSetDefaultView: boolean, selectedViewUrn: string | null | undefined) {
    useUserContextMock.mockReturnValue({
        state: { views: { hasSetDefaultView } },
        localState: { selectedViewUrn },
    } as ReturnType<typeof useUserContext>);
}

describe('isDefaultViewReady', () => {
    it('waits when no view is stored and the default has not been resolved', () => {
        expect(isDefaultViewReady(false, undefined)).toBe(false);
    });

    it('runs when a view urn is already stored', () => {
        expect(isDefaultViewReady(false, 'urn:li:dataHubView:stored')).toBe(true);
    });

    it('runs when the view was explicitly cleared', () => {
        expect(isDefaultViewReady(false, null)).toBe(true);
    });

    it('runs after resolution even when there is no default view', () => {
        expect(isDefaultViewReady(true, undefined)).toBe(true);
    });
});

describe('useIsDefaultViewReady', () => {
    beforeEach(() => {
        useUserContextMock.mockReset();
    });

    it('reads the gate from user context', () => {
        mockView(false, undefined);
        const waiting = renderHook(() => useIsDefaultViewReady());
        expect(waiting.result.current).toBe(false);

        mockView(true, undefined);
        const resolved = renderHook(() => useIsDefaultViewReady());
        expect(resolved.result.current).toBe(true);
    });
});
