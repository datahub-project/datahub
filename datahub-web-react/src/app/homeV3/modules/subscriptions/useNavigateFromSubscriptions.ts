import { useCallback } from 'react';
import { useHistory } from 'react-router';

import { useUserContext } from '@app/context/useUserContext';
import { navigateToSearchUrl } from '@app/searchV2/utils/navigateToSearchUrl';

export default function useNavigateFromSubscriptions() {
    const history = useHistory();
    const { user } = useUserContext();

    const navigateToSubscriptions = useCallback(() => {
        if (user) {
            history.push(`/settings/personal-subscriptions`);
        }
    }, [history, user]);

    const navigateToSearch = useCallback(() => {
        navigateToSearchUrl({ query: '*', history });
    }, [history]);

    return { navigateToSubscriptions, navigateToSearch };
}
