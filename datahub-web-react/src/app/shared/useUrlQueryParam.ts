import React, { useCallback, useEffect, useMemo } from 'react';
import { useHistory, useLocation } from 'react-router';

import updateQueryParams from '@app/shared/updateQueryParams';

export const useUrlQueryParam = (paramKey: string, defaultValue?: string) => {
    const location = useLocation();
    const history = useHistory();

    // Store latest location and history in refs so callbacks can access them without recreating
    const locationRef = React.useRef(location);
    const historyRef = React.useRef(history);
    locationRef.current = location;
    historyRef.current = history;

    const searchParams = useMemo(() => new URLSearchParams(location.search), [location.search]);

    const value = searchParams.get(paramKey) || defaultValue;

    useEffect(() => {
        if (!searchParams.get(paramKey) && defaultValue) {
            updateQueryParams({ [paramKey]: defaultValue }, location, history);
        }
    }, [paramKey, defaultValue, location, history, searchParams]);

    // Only depend on paramKey, use refs to access latest location/history
    const setValue = useCallback(
        (paramValue: string) => {
            updateQueryParams({ [paramKey]: paramValue }, locationRef.current, historyRef.current);
        },
        [paramKey],
    );

    return { value, setValue };
};
