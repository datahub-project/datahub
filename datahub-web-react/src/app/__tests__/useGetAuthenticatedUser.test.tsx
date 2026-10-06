import { renderHook } from '@testing-library/react-hooks';
import Cookies from 'js-cookie';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useUserContext } from '@app/context/useUserContext';
import { useGetAuthenticatedUser, useGetAuthenticatedUserUrn } from '@app/useGetAuthenticatedUser';
import { CLIENT_AUTH_COOKIE } from '@conf/Global';

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: vi.fn(),
}));

vi.mock('js-cookie', () => ({
    default: {
        get: vi.fn(),
    },
}));

const mockUseUserContext = vi.mocked(useUserContext);
const mockCookiesGet = vi.mocked(Cookies.get);

const USER_URN = 'urn:li:corpuser:jdoe';

function loadedUserContext(overrides: Record<string, unknown> = {}) {
    return {
        loaded: true,
        urn: USER_URN,
        user: {
            urn: USER_URN,
            username: 'jdoe',
        },
        platformPrivileges: {
            manageGlossaries: true,
            manageGlobalSettings: false,
        },
        ...overrides,
    } as any;
}

describe('useGetAuthenticatedUser', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('returns undefined when UserContext has not loaded', () => {
        mockUseUserContext.mockReturnValue({
            loaded: false,
            user: undefined,
            platformPrivileges: undefined,
        } as any);

        const { result } = renderHook(() => useGetAuthenticatedUser());
        expect(result.current).toBeUndefined();
    });

    it('returns undefined when loaded but user is missing', () => {
        mockUseUserContext.mockReturnValue({
            loaded: true,
            user: undefined,
            platformPrivileges: undefined,
        } as any);

        const { result } = renderHook(() => useGetAuthenticatedUser());
        expect(result.current).toBeUndefined();
    });

    it('maps context user/privileges to the historical getMe shape', () => {
        mockUseUserContext.mockReturnValue(loadedUserContext());

        const { result } = renderHook(() => useGetAuthenticatedUser());

        expect(result.current).toEqual({
            corpUser: {
                urn: USER_URN,
                username: 'jdoe',
            },
            platformPrivileges: {
                manageGlossaries: true,
                manageGlobalSettings: false,
            },
        });
    });

    it('reads user details from UserContext instead of a separate getMe query', () => {
        mockUseUserContext.mockReturnValue(loadedUserContext());

        renderHook(() => useGetAuthenticatedUser());

        expect(mockCookiesGet).not.toHaveBeenCalled();
        expect(mockUseUserContext).toHaveBeenCalled();
    });

    it('passes through null platformPrivileges without rewriting', () => {
        mockUseUserContext.mockReturnValue(loadedUserContext({ platformPrivileges: null }));

        const { result } = renderHook(() => useGetAuthenticatedUser());

        expect(result.current).toEqual({
            corpUser: {
                urn: USER_URN,
                username: 'jdoe',
            },
            platformPrivileges: null,
        });
    });
});

describe('useGetAuthenticatedUserUrn', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('returns the auth cookie value', () => {
        mockCookiesGet.mockReturnValue(USER_URN as any);

        const { result } = renderHook(() => useGetAuthenticatedUserUrn());

        expect(mockCookiesGet).toHaveBeenCalledWith(CLIENT_AUTH_COOKIE);
        expect(result.current).toBe(USER_URN);
    });

    it('throws when the auth cookie is missing', () => {
        mockCookiesGet.mockReturnValue(undefined as any);

        const { result } = renderHook(() => useGetAuthenticatedUserUrn());

        expect(result.error).toEqual(new Error('Could not find logged in user.'));
    });
});
