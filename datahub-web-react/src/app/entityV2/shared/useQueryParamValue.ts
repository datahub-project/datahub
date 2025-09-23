import * as QueryString from 'query-string';
import { useLocation } from 'react-router-dom';

export function useQueryParamValue(queryParam: string) {
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });

    return params[queryParam];
}
