import * as QueryString from 'query-string';

export default function getExpandedDrawerFieldPath(location: any) {
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    try {
        return decodeURIComponent(params.highlightedPath ? (params.highlightedPath as string) : '');
    } catch (ex) {
        return '';
    }
}
