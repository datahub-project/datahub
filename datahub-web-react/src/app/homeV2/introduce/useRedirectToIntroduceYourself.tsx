import { useEffect } from 'react';
import { useHistory } from 'react-router';

import { useLoadUserPersona } from '@app/homeV2/persona/useLoadUserPersona';
import { useShowIntroducePage } from '@app/useAppConfig';
import { PageRoutes } from '@conf/Global';

const SKIP_INTRODUCE_PAGE_KEY = 'skipAcrylIntroducePage';

export const useRedirectToIntroduceYourself = () => {
    const showIntroducePage = useShowIntroducePage();
    const history = useHistory();
    const { persona } = useLoadUserPersona();
    // this is only used in cypress tests right now
    const shouldSkipRedirect = localStorage.getItem(SKIP_INTRODUCE_PAGE_KEY);

    useEffect(() => {
        if (showIntroducePage && !persona && !shouldSkipRedirect) {
            history.replace(PageRoutes.INTRODUCE);
        }
    }, [persona, history, shouldSkipRedirect, showIntroducePage]);
};
