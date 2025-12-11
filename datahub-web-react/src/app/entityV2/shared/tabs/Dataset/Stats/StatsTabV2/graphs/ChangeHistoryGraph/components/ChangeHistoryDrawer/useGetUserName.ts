/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useCallback } from 'react';

import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { CorpUser } from '@src/types.generated';

export default function useGetUserName() {
    const entityRegistry = useEntityRegistryV2();

    return useCallback(
        (user: CorpUser) => {
            if (!user) return '';
            return entityRegistry.getDisplayName(user.type, user);
        },
        [entityRegistry],
    );
}
