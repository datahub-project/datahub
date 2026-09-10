import { act, renderHook } from '@testing-library/react-hooks';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import {
    RESOURCES_COLLAPSE_THRESHOLD,
    useResourcesCollapseState,
} from '@app/entityV2/shared/tabs/Documentation/components/useResourcesCollapseState';

const STORAGE_KEY = 'datahub:resources-section:expanded';

describe('useResourcesCollapseState', () => {
    beforeEach(() => {
        localStorage.clear();
    });

    afterEach(() => {
        localStorage.clear();
    });

    it('starts expanded when item count is below the threshold and no stored preference', () => {
        const { result } = renderHook(() => useResourcesCollapseState(RESOURCES_COLLAPSE_THRESHOLD - 1));
        expect(result.current.isExpanded).toBe(true);
    });

    it('starts collapsed when item count meets the threshold and no stored preference', () => {
        const { result } = renderHook(() => useResourcesCollapseState(RESOURCES_COLLAPSE_THRESHOLD));
        expect(result.current.isExpanded).toBe(false);
    });

    it('re-evaluates the auto default as itemCount changes when the user has no stored preference', () => {
        const { result, rerender } = renderHook(({ count }) => useResourcesCollapseState(count), {
            initialProps: { count: 0 },
        });
        expect(result.current.isExpanded).toBe(true);

        rerender({ count: RESOURCES_COLLAPSE_THRESHOLD });
        expect(result.current.isExpanded).toBe(false);

        rerender({ count: 1 });
        expect(result.current.isExpanded).toBe(true);
    });

    it('honors a stored expanded preference over the auto default', () => {
        localStorage.setItem(STORAGE_KEY, 'true');
        const { result } = renderHook(() => useResourcesCollapseState(RESOURCES_COLLAPSE_THRESHOLD + 10));
        expect(result.current.isExpanded).toBe(true);
    });

    it('honors a stored collapsed preference over the auto default', () => {
        localStorage.setItem(STORAGE_KEY, 'false');
        const { result } = renderHook(() => useResourcesCollapseState(1));
        expect(result.current.isExpanded).toBe(false);
    });

    it('persists the toggled state and stops responding to itemCount changes after the first toggle', () => {
        const { result, rerender } = renderHook(({ count }) => useResourcesCollapseState(count), {
            initialProps: { count: 1 },
        });
        expect(result.current.isExpanded).toBe(true);

        act(() => result.current.toggle());
        expect(result.current.isExpanded).toBe(false);
        expect(localStorage.getItem(STORAGE_KEY)).toBe('false');

        // Crossing the threshold should no longer change state, because we now have a stored preference.
        rerender({ count: RESOURCES_COLLAPSE_THRESHOLD + 5 });
        expect(result.current.isExpanded).toBe(false);
    });

    it('toggle flips the state and writes to localStorage', () => {
        const { result } = renderHook(() => useResourcesCollapseState(1));
        expect(result.current.isExpanded).toBe(true);

        act(() => result.current.toggle());
        expect(result.current.isExpanded).toBe(false);
        expect(localStorage.getItem(STORAGE_KEY)).toBe('false');

        act(() => result.current.toggle());
        expect(result.current.isExpanded).toBe(true);
        expect(localStorage.getItem(STORAGE_KEY)).toBe('true');
    });
});
