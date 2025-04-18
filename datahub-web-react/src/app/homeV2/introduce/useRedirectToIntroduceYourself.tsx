import { useEffect } from 'react';
import { useHistory } from 'react-router';
import { PageRoutes } from '../../../conf/Global';
import { useLoadUserPersona } from '../persona/useLoadUserPersona';

const SKIP_INTRODUCE_PAGE_KEY = 'skipAcrylIntroducePage';

export const useRedirectToIntroduceYourself = () => {
    const history = useHistory();
    const { persona } = useLoadUserPersona();
    // this is only used in cypress tests right now
    const shouldSkipRedirect = localStorage.getItem(SKIP_INTRODUCE_PAGE_KEY);

    useEffect(() => {
        if (!persona && !shouldSkipRedirect) {
            history.replace(PageRoutes.INTRODUCE);
        }
    }, [persona, history, shouldSkipRedirect]);
};
