import { useCallback } from 'react';
import { useHistory, useLocation } from 'react-router-dom';

import { updateUrlParam } from '@app/sharedV2/updateUrlParam';

/** URL param that turns on "Show siblings" in search, so refreshed and shared links keep it. */
export const SHOW_SIBLINGS_URL_PARAM = 'showSiblings';

/**
 * Whether search shows each result's siblings under it, and a setter that changes only this URL
 * param (with history.replace, as the sort option does). navigateToSearchUrl carries it through
 * paging, filtering and new searches.
 */
export function useShowSiblings(): [boolean, (show: boolean) => void] {
    const location = useLocation();
    const history = useHistory();
    const enabled = new URLSearchParams(location.search).get(SHOW_SIBLINGS_URL_PARAM) === 'true';

    const setEnabled = useCallback(
        (show: boolean) => updateUrlParam(history, SHOW_SIBLINGS_URL_PARAM, show ? 'true' : null),
        [history],
    );

    return [enabled, setEnabled];
}
