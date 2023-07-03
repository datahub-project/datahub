import * as QueryString from 'query-string';
import { useLocation } from 'react-router-dom';

export const SHOW_COLUMNS_URL_PARAMS = 'show_columns';

export function useIsShowColumnsMode() {
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });

    return params[SHOW_COLUMNS_URL_PARAMS] === 'true';
}
