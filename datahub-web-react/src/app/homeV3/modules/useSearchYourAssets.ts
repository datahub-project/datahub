import { useCallback } from 'react';
import { useHistory } from 'react-router';

import { useUserContext } from '@app/context/useUserContext';
import { navigateToSearchUrl } from '@app/searchV2/utils/navigateToSearchUrl';

export default function useSearchYourAssets() {
    const history = useHistory();
    const { urn } = useUserContext();

    return useCallback(() => {
        if (urn) {
            navigateToSearchUrl({ query: '*', history, filters: [{ field: 'owners', values: [urn] }] });
        }
    }, [history, urn]);
}
