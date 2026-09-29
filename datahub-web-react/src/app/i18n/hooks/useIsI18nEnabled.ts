import { useFeatureFlag } from '@app/sharedV2/hooks/useFeatureFlag';

export function useIsI18nEnabled() {
    return useFeatureFlag('i18nEnabled');
}
