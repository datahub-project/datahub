import * as QueryString from 'query-string';
import { useLocation } from 'react-router-dom';

export default function useGetSemanticVersionFromUrlParams() {
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const semanticVersion: string = params.semantic_version as string;
    return semanticVersion;
}
