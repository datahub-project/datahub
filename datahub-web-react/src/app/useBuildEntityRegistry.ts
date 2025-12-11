/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useMemo } from 'react';

import buildEntityRegistry from '@app/buildEntityRegistry';
import buildEntityRegistryV2 from '@app/buildEntityRegistryV2';
import { useIsThemeV2 } from '@app/useIsThemeV2';

export default function useBuildEntityRegistry() {
    const isThemeV2Enabled = useIsThemeV2();
    return useMemo(() => {
        return isThemeV2Enabled ? buildEntityRegistryV2() : buildEntityRegistry();
    }, [isThemeV2Enabled]);
}
