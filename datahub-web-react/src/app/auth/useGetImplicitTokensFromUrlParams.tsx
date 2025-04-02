import * as QueryString from 'query-string';
import { useLocation } from 'react-router-dom';

export default function useGetImplicitTokensFromUrlParams() {
    const location = useLocation();
    const params = QueryString.parse(location.hash, { arrayFormat: 'comma' });
    const accessToken: string = params.access_token as string;
    const idToken: string = params.id_token as string;
    return { accessToken, idToken };
}
