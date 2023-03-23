import { useHistory, useLocation } from 'react-router';
import * as QueryString from 'query-string';
import { useEffect } from 'react';

export default function useUpdateSchemaFilterQueryString(filterText: string) {
    const location = useLocation();
    const history = useHistory();
    const parsedParams = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const newParams = {
        ...parsedParams,
        schemaFilter: filterText,
    };
    const stringifiedParams = QueryString.stringify(newParams, { arrayFormat: 'comma' });

    useEffect(() => {
        history.replace({
            pathname: location.pathname,
            search: stringifiedParams,
        });
    }, [filterText, history, location.pathname, stringifiedParams]);
}
