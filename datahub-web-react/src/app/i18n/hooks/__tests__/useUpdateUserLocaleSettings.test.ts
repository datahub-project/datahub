import { act } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useUserContext } from '@app/context/useUserContext';
import { useUpdateUserLocaleSettings } from '@app/i18n/hooks/useUpdateUserLocaleSettings';
import { renderHook } from '@utils/test-utils/renderHook';

import { useUpdateCorpUserLocaleSettingsMutation } from '@graphql/user.generated';

vi.mock('@app/context/useUserContext');
vi.mock('@graphql/user.generated');

const mockUpdateLocaleSettings = vi.fn().mockResolvedValue({});
const mockRefetchUser = vi.fn().mockResolvedValue({});

const mockUseUserContext = vi.mocked(useUserContext);
const mockUseMutation = vi.mocked(useUpdateCorpUserLocaleSettingsMutation);

describe('useUpdateUserLocaleSettings', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockUseUserContext.mockReturnValue({ refetchUser: mockRefetchUser } as any);
        mockUseMutation.mockReturnValue([mockUpdateLocaleSettings] as any);
    });

    it('calls mutation with the given language', async () => {
        const { result } = renderHook(() => useUpdateUserLocaleSettings());

        await act(async () => {
            await result.current('en');
        });

        expect(mockUpdateLocaleSettings).toHaveBeenCalledWith({ variables: { input: { language: 'en' } } });
    });

    it('calls refetchUser after the mutation', async () => {
        const { result } = renderHook(() => useUpdateUserLocaleSettings());

        await act(async () => {
            await result.current('en');
        });

        expect(mockRefetchUser).toHaveBeenCalled();
    });

    it('passes null language to the mutation', async () => {
        const { result } = renderHook(() => useUpdateUserLocaleSettings());

        await act(async () => {
            await result.current(null);
        });

        expect(mockUpdateLocaleSettings).toHaveBeenCalledWith({ variables: { input: { language: null } } });
    });

    it('resolves when the mutation succeeds but refetching the user fails', async () => {
        mockRefetchUser.mockRejectedValueOnce(new Error('network'));
        vi.spyOn(console, 'error').mockImplementation(() => {});
        const { result } = renderHook(() => useUpdateUserLocaleSettings());

        await act(async () => {
            await expect(result.current('de')).resolves.toBeUndefined();
        });
    });

    it('rejects when the mutation fails', async () => {
        mockUpdateLocaleSettings.mockRejectedValueOnce(new Error('save failed'));
        const { result } = renderHook(() => useUpdateUserLocaleSettings());

        await act(async () => {
            await expect(result.current('de')).rejects.toThrow();
        });

        expect(mockRefetchUser).not.toHaveBeenCalled();
    });
});
