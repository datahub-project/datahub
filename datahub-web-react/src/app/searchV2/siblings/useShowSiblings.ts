import { useCallback } from 'react';
import { useHistory, useLocation } from 'react-router-dom';

/** URL param that turns on "Show siblings" in search, so refreshed and shared links keep it. */
export const SHOW_SIBLINGS_URL_PARAM = 'showSiblings';

/**
 * Whether search shows each result's siblings under it, and a setter that updates only this URL
 * param. Other params (query, filters, page) are left exactly as they are; filter values can hold
 * urns with commas, so the search string is edited with URLSearchParams rather than re-serialized.
 */
export function useShowSiblings(): [boolean, (show: boolean) => void] {
    const location = useLocation();
    const history = useHistory();
    const enabled = new URLSearchParams(location.search).get(SHOW_SIBLINGS_URL_PARAM) === 'true';

    const setEnabled = useCallback(
        (show: boolean) => {
            const params = new URLSearchParams(location.search);
            if (show) {
                params.set(SHOW_SIBLINGS_URL_PARAM, 'true');
            } else {
                params.delete(SHOW_SIBLINGS_URL_PARAM);
            }
            const search = params.toString();
            history.push({ pathname: location.pathname, search: search ? `?${search}` : '' });
        },
        [history, location.pathname, location.search],
    );

    return [enabled, setEnabled];
}
