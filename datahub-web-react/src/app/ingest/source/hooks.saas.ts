import * as QueryString from 'query-string';
import { useHistory, useLocation } from 'react-router';
import { INGESTION_TAB_QUERY_PARAMS } from '../constants';
import { TabType } from '../types';

export const usePoolActionsForIngestionSourceList = (params, onSwitchTab: (tab: string) => void) => {
    const history = useHistory();
    const location = useLocation();

    const clearPoolFilter = () => {
        const newParams = { ...params };
        delete newParams[INGESTION_TAB_QUERY_PARAMS.pool];
        const newSearch = QueryString.stringify(newParams);
        history.push(`${location.pathname}${newSearch ? `?${newSearch}` : ''}`);
    };

    const onViewPool = (poolId: string) => {
        const newParams = { [INGESTION_TAB_QUERY_PARAMS.pool]: poolId };
        const newSearch = QueryString.stringify(newParams);
        history.push(`${location.pathname}${`?${newSearch}`}`);
        setTimeout(() => onSwitchTab(TabType.RemoteExecutors), 0);
    };

    return { clearPoolFilter, onViewPool };
};
