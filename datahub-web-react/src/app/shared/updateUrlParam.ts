import * as QueryString from 'query-string';
import { RouteComponentProps } from 'react-router';

export function updateUrlParam(history: RouteComponentProps['history'], key: string, value: string, state?: any) {
    // Parse existing params without decoding so percent-encoded values (e.g. filter_degree=3%2B) are
    // preserved byte-for-byte. The native URL.searchParams API would decode %2B → + and re-encode it
    // as %20 (space), which breaks the GMS degree filter validation.
    const parsedParams = QueryString.parse(window.location.search, { arrayFormat: 'comma', decode: false });
    const updatedParams = { ...parsedParams, [key]: encodeURIComponent(value) };
    const newSearch = QueryString.stringify(updatedParams, { arrayFormat: 'comma', encode: false });
    history.replace(`?${newSearch}`, state);
}
