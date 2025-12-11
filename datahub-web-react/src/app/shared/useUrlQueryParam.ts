/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEffect, useMemo } from 'react';
import { useHistory, useLocation } from 'react-router';

import updateQueryParams from '@app/shared/updateQueryParams';

export const useUrlQueryParam = (paramKey: string, defaultValue?: string) => {
    const location = useLocation();
    const history = useHistory();

    const searchParams = useMemo(() => new URLSearchParams(location.search), [location.search]);

    const value = searchParams.get(paramKey) || defaultValue;

    useEffect(() => {
        if (!searchParams.get(paramKey) && defaultValue) {
            updateQueryParams({ [paramKey]: defaultValue }, location, history);
        }
    }, [paramKey, defaultValue, location, history, searchParams]);

    const setValue = (paramValue: string) => {
        updateQueryParams({ [paramKey]: paramValue }, location, history);
    };

    return { value, setValue };
};
