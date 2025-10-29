import { useMemo } from 'react';

import buildEntityRegistryV2 from '@app/buildEntityRegistryV2';

export default function useBuildEntityRegistry() {
    return useMemo(() => {
        return buildEntityRegistryV2();
    }, []);
}
