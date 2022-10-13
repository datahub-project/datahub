import * as QueryString from 'query-string';

export default function getSchemaFilterFromQueryString(location: any) {
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    return decodeURIComponent(params.schemaFilter ? (params.schemaFilter as string) : '');
}
