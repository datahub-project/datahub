import { useCallback } from 'react';
import { useHistory } from 'react-router';

import { useUserContext } from '@app/context/useUserContext';
import useGetUserGroupUrns from '@app/entityV2/user/useGetUserGroupUrns';
import { navigateToSearchUrl } from '@app/searchV2/utils/navigateToSearchUrl';

export default function useSearchYourAssets() {
    const history = useHistory();
    const { urn } = useUserContext();
    const { groupUrns } = useGetUserGroupUrns(urn ?? undefined);

    return useCallback(() => {
        if (urn) {
            navigateToSearchUrl({ query: '*', history, filters: [{ field: 'owners', values: [urn, ...groupUrns] }] });
        }
    }, [groupUrns, history, urn]);
}
