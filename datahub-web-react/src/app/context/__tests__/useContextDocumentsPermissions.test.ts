import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { vi } from 'vitest';

import { useContextDocumentsPermissions } from '@app/context/useContextDocumentsPermissions';
import { UserContext } from '@app/context/userContext';

// Mock user context values
const createMockUserContext = (
    overrides: Partial<{
        platformPrivileges: { manageDocuments: boolean };
        loaded: boolean;
    }> = {},
) => ({
    urn: 'urn:li:corpuser:test',
    user: null,
    state: {},
    localState: {},
    updateLocalState: vi.fn(),
    updateState: vi.fn(),
    platformPrivileges: overrides.platformPrivileges || null,
    loaded: overrides.loaded ?? true,
});

describe('useContextDocumentsPermissions', () => {
    it('should return false for all permissions when user context is not loaded', () => {
        const mockContext = createMockUserContext({ loaded: false });

        const wrapper = ({ children }: { children: React.ReactNode }) =>
            React.createElement(UserContext.Provider, { value: mockContext as any }, children);

        const { result } = renderHook(() => useContextDocumentsPermissions(), { wrapper });

        expect(result.current.canCreate).toBe(false);
        expect(result.current.canManage).toBe(false);
    });

    it('should return false for all permissions when user has no platform privileges', () => {
        const mockContext = createMockUserContext({
            loaded: true,
            platformPrivileges: undefined as any,
        });

        const wrapper = ({ children }: { children: React.ReactNode }) =>
            React.createElement(UserContext.Provider, { value: mockContext as any }, children);

        const { result } = renderHook(() => useContextDocumentsPermissions(), { wrapper });

        expect(result.current.canCreate).toBe(false);
        expect(result.current.canManage).toBe(false);
    });

    it('should return false for all permissions when manageDocuments is false', () => {
        const mockContext = createMockUserContext({
            loaded: true,
            platformPrivileges: { manageDocuments: false },
        });

        const wrapper = ({ children }: { children: React.ReactNode }) =>
            React.createElement(UserContext.Provider, { value: mockContext as any }, children);

        const { result } = renderHook(() => useContextDocumentsPermissions(), { wrapper });

        expect(result.current.canCreate).toBe(false);
        expect(result.current.canManage).toBe(false);
    });

    it('should return true for all permissions when manageDocuments is true', () => {
        const mockContext = createMockUserContext({
            loaded: true,
            platformPrivileges: { manageDocuments: true },
        });

        const wrapper = ({ children }: { children: React.ReactNode }) =>
            React.createElement(UserContext.Provider, { value: mockContext as any }, children);

        const { result } = renderHook(() => useContextDocumentsPermissions(), { wrapper });

        expect(result.current.canCreate).toBe(true);
        expect(result.current.canManage).toBe(true);
    });
});
