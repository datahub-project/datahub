import * as QueryString from 'query-string';
import { useLocation } from 'react-router-dom';

export default function useGetInviteTokenFromUrlParams() {
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const inviteToken: string = params.invite_token as string;
    return inviteToken;
}
