import { useMemo } from 'react';
import buildEntityRegistry from './buildEntityRegistry';
import buildEntityRegistryV2 from './buildEntityRegistryV2';
import { useIsThemeV2Enabled } from './useIsThemeV2Enabled';

export default function useBuildEntityRegistry() {
    const isThemeV2Enabled = useIsThemeV2Enabled();
    return useMemo(() => {
        return isThemeV2Enabled ? buildEntityRegistryV2() : buildEntityRegistry();
    }, [isThemeV2Enabled]);
}
