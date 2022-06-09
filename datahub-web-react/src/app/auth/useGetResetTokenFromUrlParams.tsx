import * as QueryString from 'query-string';
import { useLocation } from 'react-router-dom';

export default function useGetResetTokenFromUrlParams() {
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const resetToken: string = params.reset_token as string;
    return resetToken;
}
