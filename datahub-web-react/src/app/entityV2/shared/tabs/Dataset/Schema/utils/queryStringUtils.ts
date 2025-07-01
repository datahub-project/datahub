import * as QueryString from 'query-string';

export function getSchemaFilterFromQueryString(location: any) {
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    return decodeURIComponent(params.schemaFilter ? (params.schemaFilter as string) : '');
}

export function getMatchedTextFromQueryString(location: any) {
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    return decodeURIComponent(params.matchedText ? (params.matchedText as string) : '');
}
