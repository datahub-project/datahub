/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useHistory } from 'react-router';

import { PLATFORM_FILTER_NAME } from '@app/search/utils/constants';
import { navigateToSearchUrl } from '@app/searchV2/utils/navigateToSearchUrl';
import { PageRoutes } from '@conf/Global';

import { Entity } from '@types';

const usePlatformsModuleUtils = () => {
    const history = useHistory();

    const navigateToDataSources = () => {
        history.push({
            pathname: `${PageRoutes.INGESTION}`,
        });
    };

    const handleEntityClick = (entity: Entity) => {
        if (!entity?.urn) return;
        navigateToSearchUrl({
            history,
            filters: [
                {
                    field: PLATFORM_FILTER_NAME,
                    values: [entity.urn],
                },
            ],
        });
    };

    return { navigateToDataSources, handleEntityClick };
};

export default usePlatformsModuleUtils;
