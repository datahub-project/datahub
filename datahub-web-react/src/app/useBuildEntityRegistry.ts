import { useMemo } from 'react';
import buildEntityRegistry from './buildEntityRegistry';
import { useTranslation } from 'react-i18next';

export default function useBuildEntityRegistry() {
    const { t } = useTranslation();

    return useMemo(() => {
        return buildEntityRegistry(t);
    }, []);
}
