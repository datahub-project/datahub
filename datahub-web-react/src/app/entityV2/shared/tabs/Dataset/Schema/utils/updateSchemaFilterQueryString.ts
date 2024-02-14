import { useHistory, useLocation } from 'react-router';
import * as QueryString from 'query-string';
import { useEffect } from 'react';
import { SchemaFilterType } from './filterSchemaRows';

export default function useUpdateSchemaFilterQueryString(
    filterText: string,
    expandedDrawerFieldPath: string | null,
    schemaFilterTypes: SchemaFilterType[],
) {
    const location = useLocation();
    const history = useHistory();
    const parsedParams = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const newParams = {
        ...parsedParams,
        schemaFilter: filterText,
        highlightedPath: expandedDrawerFieldPath,
        schemaFilterTypes: schemaFilterTypes.length === 4 ? [] : schemaFilterTypes,
    };
    const stringifiedParams = QueryString.stringify(newParams, { arrayFormat: 'comma' });

    useEffect(() => {
        history.replace({
            pathname: location.pathname,
            search: stringifiedParams,
        });
    }, [filterText, history, location.pathname, stringifiedParams, expandedDrawerFieldPath, schemaFilterTypes]);
}
