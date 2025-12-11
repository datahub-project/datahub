/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
