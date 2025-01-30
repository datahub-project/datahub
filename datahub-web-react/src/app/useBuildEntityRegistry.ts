import { useMemo } from 'react';
import buildEntityRegistry from './buildEntityRegistry';
import buildEntityRegistryV2 from './buildEntityRegistryV2';
import { useIsThemeV2 } from './useIsThemeV2';

export default function useBuildEntityRegistry() {
    const isThemeV2Enabled = useIsThemeV2();
    return useMemo(() => {
        return isThemeV2Enabled ? buildEntityRegistryV2() : buildEntityRegistry();
    }, [isThemeV2Enabled]);
}
