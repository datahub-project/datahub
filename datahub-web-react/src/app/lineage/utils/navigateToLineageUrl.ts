/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import * as QueryString from 'query-string';
import { RouteComponentProps } from 'react-router-dom';

import { SEPARATE_SIBLINGS_URL_PARAM } from '@app/entity/shared/siblingUtils';
import { SHOW_COLUMNS_URL_PARAMS } from '@app/lineage/utils/useIsShowColumnsMode';

export const navigateToLineageUrl = ({
    location,
    history,
    isLineageMode,
    isHideSiblingMode,
    showColumns,
    startTimeMillis,
    endTimeMillis,
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
}) => {
    const parsedSearch = QueryString.parse(location.search, { arrayFormat: 'comma' });
    let newSearch: any = {
        ...parsedSearch,
        is_lineage_mode: isLineageMode,
        start_time_millis: startTimeMillis || null,
        end_time_millis: endTimeMillis || null,
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
