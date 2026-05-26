import { useCallback } from 'react';

import { useUserContext } from '@app/context/useUserContext';

import { useUpdateCorpUserLocaleSettingsMutation } from '@graphql/user.generated';

export function useUpdateUserLocaleSettings() {
    const [updateLocaleSettings] = useUpdateCorpUserLocaleSettingsMutation();
    const { refetchUser } = useUserContext();

    return useCallback(
        async (language: string | null) => {
            await updateLocaleSettings({ variables: { input: { language } } });
            await refetchUser?.();
        },
        [updateLocaleSettings, refetchUser],
    );
}
