import { useCallback } from 'react';
import { useHistory } from 'react-router';

import { useUserContext } from '@app/context/useUserContext';
import { navigateToSearchUrl } from '@app/searchV2/utils/navigateToSearchUrl';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { EntityType } from '@types';

export default function useNavigateFromSubscriptions() {
    const history = useHistory();
    const { user } = useUserContext();
    const entityRegistry = useEntityRegistryV2();

    const navigateToSubscriptions = useCallback(() => {
        if (user) {
            history.push(`${entityRegistry.getEntityUrl(EntityType.CorpUser, user?.urn)}/subscriptions`);
        }
    }, [entityRegistry, history, user]);

    const navigateToSearch = useCallback(() => {
        navigateToSearchUrl({ query: '*', history });
    }, [history]);

    return { navigateToSubscriptions, navigateToSearch };
}
