import { useMemo } from 'react';
import buildEntityRegistry from './buildEntityRegistry';

export default function useBuildEntityRegistry() {
    return useMemo(() => {
        return buildEntityRegistry();
    }, []);
}
