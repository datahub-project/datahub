import * as QueryString from 'query-string';
import { useCallback } from 'react';
import { useHistory, useLocation } from 'react-router';

import { INGESTION_TAB_QUERY_PARAMS } from '@app/ingestV2/constants';
import { TabType, tabUrlMap } from '@app/ingestV2/types';

export const usePoolActionsForIngestionSourceList = (params, shouldPreserveParams) => {
    const history = useHistory();
    const location = useLocation();

    const clearPoolFilter = useCallback(() => {
        const newParams = { ...params };
        delete newParams[INGESTION_TAB_QUERY_PARAMS.pool];
        const newSearch = QueryString.stringify(newParams);
        history.push(`${location.pathname}${newSearch ? `?${newSearch}` : ''}`);
    }, [params, history, location]);

    const onViewPool = useCallback(
        (poolId: string) => {
            const preserveParams = shouldPreserveParams;
            preserveParams.current = true;
            const newParams = { [INGESTION_TAB_QUERY_PARAMS.pool]: poolId };
            history.replace({
                pathname: tabUrlMap[TabType.RemoteExecutors],
                search: QueryString.stringify(newParams),
            });
        },
        [shouldPreserveParams, history],
    );

    return { clearPoolFilter, onViewPool };
};
