import React from 'react';
import { Affix } from 'antd';
import { useLocation, useParams } from 'react-router';

import { SearchablePage } from '../search/SearchablePage';
import { PLATFORM_FILTER_NAME } from '../search/utils/constants';
import { useEntityRegistry } from '../useEntityRegistry';
import { MetaDataContainers } from './MetaDataContainers';
import { LegacyMetaDataPath } from './LegacyMetaDataPath';
import { MetaDataPlatforms } from './MetaDataPlatforms';

type MetaDataParentPageParams = {
    type: string;
};

export const MetaDataResultsPage = () => {
    const entityRegistry = useEntityRegistry();
    const location = useLocation();
    const rootPath = location.pathname;
    const path = rootPath.split('/').slice(3);

    const { type } = useParams<MetaDataParentPageParams>();
    const entityType = entityRegistry.getTypeFromPathName(type);

    return (
        <SearchablePage>
            <Affix offsetTop={60}>
                <LegacyMetaDataPath type={entityType} path={path} isBrowsable />
            </Affix>
            {path.length === 0 ? (
                <MetaDataPlatforms rootPath={rootPath} entityType={entityType} />
            ) : (
                <MetaDataContainers
                    fixedFilter={{ field: PLATFORM_FILTER_NAME, value: path[0] }}
                    rootPath={rootPath}
                    entityRegistry={entityRegistry}
                    emptySearchQuery="*"
                    placeholderText="Filter entities..."
                    entityType={entityType}
                    fixedQuery="!hasContainer:true"
                />
            )}
        </SearchablePage>
    );
};
