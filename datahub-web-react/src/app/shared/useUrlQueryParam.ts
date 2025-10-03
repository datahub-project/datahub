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
