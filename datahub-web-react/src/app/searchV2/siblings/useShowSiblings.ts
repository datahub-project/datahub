import { useCallback } from 'react';
import { useHistory, useLocation } from 'react-router-dom';

import { updateUrlParam } from '@app/sharedV2/updateUrlParam';
import { useIsShowSeparateSiblingsEnabled, useMaxSiblingGroupUrns } from '@app/useAppConfig';

/** URL param that turns on "Show siblings" in search, so refreshed and shared links keep it. */
export const SHOW_SIBLINGS_URL_PARAM = 'showSiblings';

/**
 * Whether "Show siblings" is available: siblings are shown as separate results, and the server's
 * siblingGroups query is on (its urn limit is above 0).
 */
export function useIsShowSiblingsAvailable(): boolean {
    const separateSiblings = useIsShowSeparateSiblingsEnabled();
    const maxUrns = useMaxSiblingGroupUrns();
    return separateSiblings && maxUrns > 0;
}

/**
 * Whether search shows each result's siblings under it, and a setter that changes only this URL
 * param (with history.replace, as the sort option does). navigateToSearchUrl carries it through
 * paging, filtering and new searches.
 *
 * The feature exists only where siblings are shown as separate results and the server's
 * siblingGroups query is on. In the combined view the result cards already merge siblings, so the
 * param is ignored there.
 */
export function useShowSiblings(): [boolean, (show: boolean) => void] {
    const location = useLocation();
    const history = useHistory();
    const available = useIsShowSiblingsAvailable();
    const enabled = available && new URLSearchParams(location.search).get(SHOW_SIBLINGS_URL_PARAM) === 'true';

    const setEnabled = useCallback(
        (show: boolean) => updateUrlParam(history, SHOW_SIBLINGS_URL_PARAM, show ? 'true' : null),
        [history],
    );

    return [enabled, setEnabled];
}
