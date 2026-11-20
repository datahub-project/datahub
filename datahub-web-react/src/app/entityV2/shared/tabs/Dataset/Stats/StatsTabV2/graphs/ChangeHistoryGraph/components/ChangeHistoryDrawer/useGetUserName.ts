import { useCallback } from 'react';

import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { CorpUser } from '@src/types.generated';

export default function useGetUserName() {
    const entityRegistry = useEntityRegistry();

    return useCallback(
        (user: CorpUser) => {
            if (!user) return '';
            return entityRegistry.getDisplayName(user.type, user);
        },
        [entityRegistry],
    );
}
