import { act, renderHook } from '@testing-library/react-hooks';

import { useReloadableModules } from '@app/homeV3/module/context/hooks/useReloadableModules';

import { DataHubPageModuleType } from '@types';

describe('useReloadableModules', () => {
    beforeEach(() => {
        vi.useFakeTimers();
    });

    afterEach(() => {
        vi.runOnlyPendingTimers();
        vi.useRealTimers();
    });

    it('should initially have no loaded modules', () => {
        const { result } = renderHook(() => useReloadableModules());
        expect(result.current.shouldModuleBeReloaded(DataHubPageModuleType.Link, 'urn:li:glossaryNode:1')).toBe(true);
    });

    it('should mark a module as reloaded', () => {
        const { result } = renderHook(() => useReloadableModules());

        act(() => {
            result.current.markModulesAsReloaded(DataHubPageModuleType.Link, 'urn:li:glossaryNode:1');
        });

        expect(result.current.shouldModuleBeReloaded(DataHubPageModuleType.Link, 'urn:li:glossaryNode:1')).toBe(false);
    });

    it('should reload modules immediately when intervalMs is 0', () => {
        const { result } = renderHook(() => useReloadableModules());

        act(() => {
            result.current.markModulesAsReloaded(DataHubPageModuleType.Link, 'urn:li:glossaryNode:1');
            result.current.markModulesAsReloaded(DataHubPageModuleType.Domains, 'urn:li:domain:2');
        });

        expect(result.current.shouldModuleBeReloaded(DataHubPageModuleType.Link, 'urn:li:glossaryNode:1')).toBe(false);
        expect(result.current.shouldModuleBeReloaded(DataHubPageModuleType.Domains, 'urn:li:domain:2')).toBe(false);

        act(() => {
            result.current.reloadModules([DataHubPageModuleType.Link]);
        });

        expect(result.current.shouldModuleBeReloaded(DataHubPageModuleType.Link, 'urn:li:glossaryNode:1')).toBe(true);
        expect(result.current.shouldModuleBeReloaded(DataHubPageModuleType.Domains, 'urn:li:domain:2')).toBe(false);
    });

    it('should reload modules after a specified interval', () => {
        const { result } = renderHook(() => useReloadableModules());

        act(() => {
            result.current.markModulesAsReloaded(DataHubPageModuleType.Link, 'urn:li:glossaryNode:1');
        });

        expect(result.current.shouldModuleBeReloaded(DataHubPageModuleType.Link, 'urn:li:glossaryNode:1')).toBe(false);

        act(() => {
            result.current.reloadModules([DataHubPageModuleType.Link], 1000);
        });

        expect(result.current.shouldModuleBeReloaded(DataHubPageModuleType.Link, 'urn:li:glossaryNode:1')).toBe(false);

        act(() => {
            vi.advanceTimersByTime(1000);
        });

        expect(result.current.shouldModuleBeReloaded(DataHubPageModuleType.Link, 'urn:li:glossaryNode:1')).toBe(true);
    });

    it('should handle multiple module types for reloading', () => {
        const { result } = renderHook(() => useReloadableModules());

        act(() => {
            result.current.markModulesAsReloaded(DataHubPageModuleType.Link, 'urn:li:glossaryNode:1');
            result.current.markModulesAsReloaded(DataHubPageModuleType.Domains, 'urn:li:domain:2');
            result.current.markModulesAsReloaded(DataHubPageModuleType.DataProducts, 'urn:li:dataProduct:3');
        });

        expect(result.current.shouldModuleBeReloaded(DataHubPageModuleType.Link, 'urn:li:glossaryNode:1')).toBe(false);
        expect(result.current.shouldModuleBeReloaded(DataHubPageModuleType.Domains, 'urn:li:domain:2')).toBe(false);
        expect(result.current.shouldModuleBeReloaded(DataHubPageModuleType.DataProducts, 'urn:li:dataProduct:3')).toBe(
            false,
        );

        act(() => {
            result.current.reloadModules([DataHubPageModuleType.Link, DataHubPageModuleType.Domains]);
        });

        expect(result.current.shouldModuleBeReloaded(DataHubPageModuleType.Link, 'urn:li:glossaryNode:1')).toBe(true);
        expect(result.current.shouldModuleBeReloaded(DataHubPageModuleType.Domains, 'urn:li:domain:2')).toBe(true);
        expect(result.current.shouldModuleBeReloaded(DataHubPageModuleType.DataProducts, 'urn:li:dataProduct:3')).toBe(
            false,
        );
    });

    it('should correctly identify if a module with a specific URN should be reloaded', () => {
        const { result } = renderHook(() => useReloadableModules());

        act(() => {
            result.current.markModulesAsReloaded(DataHubPageModuleType.Link, 'urn:li:glossaryNode:1');
            result.current.markModulesAsReloaded(DataHubPageModuleType.Link, 'urn:li:glossaryNode:2');
        });

        expect(result.current.shouldModuleBeReloaded(DataHubPageModuleType.Link, 'urn:li:glossaryNode:1')).toBe(false);
        expect(result.current.shouldModuleBeReloaded(DataHubPageModuleType.Link, 'urn:li:glossaryNode:2')).toBe(false);
        expect(result.current.shouldModuleBeReloaded(DataHubPageModuleType.Link, 'urn:li:glossaryNode:3')).toBe(true);
    });
});
