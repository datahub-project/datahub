import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import buildEntityRegistry from './buildEntityRegistry';

export default function useBuildEntityRegistry() {
    const { t } = useTranslation();

    return useMemo(() => {
        return buildEntityRegistry(t);
    }, [t]);
}
