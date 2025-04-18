import * as QueryString from 'query-string';
import { RouteComponentProps } from 'react-router-dom';
import { SEPARATE_SIBLINGS_URL_PARAM } from '../../entity/shared/siblingUtils';
import { SHOW_COLUMNS_URL_PARAMS } from './useIsShowColumnsMode';

export const navigateToLineageUrl = ({
    location,
    history,
    isLineageMode,
    isHideSiblingMode,
    showColumns,
    startTimeMillis,
    endTimeMillis,
    showAllTimeLineage,
}: {
    location: {
        search: string;
        pathname: string;
    };
    history: RouteComponentProps['history'];
    isLineageMode: boolean;
    isHideSiblingMode?: boolean;
    showColumns?: boolean;
    startTimeMillis?: number;
    endTimeMillis?: number;
    showAllTimeLineage?: boolean;
}) => {
    const parsedSearch = QueryString.parse(location.search, { arrayFormat: 'comma' });
    let newSearch: any = {
        ...parsedSearch,
        is_lineage_mode: isLineageMode,
        start_time_millis: startTimeMillis || null,
        end_time_millis: endTimeMillis || null,
        show_all_time_lineage: showAllTimeLineage || null,
    };
    if (isHideSiblingMode !== undefined) {
        newSearch = {
            ...newSearch,
            [SEPARATE_SIBLINGS_URL_PARAM]: isHideSiblingMode,
        };
    }
    if (showColumns !== undefined) {
        newSearch = {
            ...newSearch,
            [SHOW_COLUMNS_URL_PARAMS]: showColumns,
        };
    }
    const newSearchStringified = QueryString.stringify(newSearch, { arrayFormat: 'comma' });

    history.replace({
        pathname: location.pathname,
        search: newSearchStringified,
    });
};
