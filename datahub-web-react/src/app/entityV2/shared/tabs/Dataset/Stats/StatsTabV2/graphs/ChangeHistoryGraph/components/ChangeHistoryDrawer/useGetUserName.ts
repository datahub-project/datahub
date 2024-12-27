import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { CorpUser } from '@src/types.generated';
import { useCallback } from 'react';

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
