import { useCallback } from 'react';

import { useUserContext } from '@app/context/useUserContext';

import { useUpdateCorpUserLocaleSettingsMutation } from '@graphql/user.generated';

export function useUpdateUserLocaleSettings() {
    const [updateLocaleSettings] = useUpdateCorpUserLocaleSettingsMutation();
    const { refetchUser } = useUserContext();

    return useCallback(
        async (language: string | null) => {
            await updateLocaleSettings({ variables: { input: { language } } });
            // The language is saved once the mutation succeeds. A failed refetch only leaves the
            // cached user stale, so it must not stop the caller from switching the UI or tracking
            // the change. useLanguageSync only re-applies the language when the cached value
            // changes, so the stale value does not switch the user back.
            try {
                await refetchUser?.();
            } catch (error) {
                console.error('Failed to refetch user after updating locale settings', error);
            }
        },
        [updateLocaleSettings, refetchUser],
    );
}
