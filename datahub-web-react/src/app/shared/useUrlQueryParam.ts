import { useCallback, useEffect, useMemo } from 'react';
import { useHistory, useLocation } from 'react-router';

import { updateUrlParam } from '@app/shared/updateUrlParam';

export const useUrlQueryParam = (paramKey: string, defaultValue?: string) => {
    const location = useLocation();
    const history = useHistory();

    const searchParams = useMemo(() => new URLSearchParams(location.search), [location.search]);

    const value = searchParams.get(paramKey) || defaultValue;
    const locationState = location.state;

    useEffect(() => {
        if (!searchParams.get(paramKey) && defaultValue) {
            updateUrlParam(history, paramKey, defaultValue, locationState);
        }
    }, [paramKey, defaultValue, history, searchParams, locationState]);

    const setValue = useCallback(
        (paramValue: string) => {
            updateUrlParam(history, paramKey, paramValue, locationState);
        },
        [paramKey, history, locationState],
    );

    return { value, setValue };
};
