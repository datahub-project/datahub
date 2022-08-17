import React from 'react';
import { Redirect, useHistory, useLocation, useParams } from 'react-router';
import * as QueryString from 'query-string';
import { Affix, Alert } from 'antd';
import { BrowseCfg } from '../../conf';
import { BrowseResults } from './BrowseResults';
import { useGetBrowseResultsQuery } from '../../graphql/browse.generated';
import { LegacyBrowsePath } from './LegacyBrowsePath';
import { PageRoutes } from '../../conf/Global';
import { useEntityRegistry } from '../useEntityRegistry';
import { Message } from '../shared/Message';

type BrowseResultsPageParams = {
    type: string;
};

export const BrowseResultsPage = () => {
    const location = useLocation();
    const history = useHistory();
    const { type } = useParams<BrowseResultsPageParams>();

    const entityRegistry = useEntityRegistry();

    const rootPath = location.pathname;
    const params = QueryString.parse(location.search);
    const entityType = entityRegistry.getTypeFromPathName(type);
    const path = rootPath.split('/').slice(3);
    const page: number = params.page && Number(params.page as string) > 0 ? Number(params.page as string) : 1;

    const { data, loading, error } = useGetBrowseResultsQuery({
        variables: {
            input: {
                type: entityType,
                path,
                start: (page - 1) * BrowseCfg.RESULTS_PER_PAGE,
                count: BrowseCfg.RESULTS_PER_PAGE,
                filters: null,
            },
        },
    });

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    const onChangePage = (newPage: number) => {
        history.push({
            pathname: rootPath,
            search: `&page=${newPage}`,
        });
    };

    if (page < 0 || page === undefined || Number.isNaN(page)) {
        return <Redirect to={`${PageRoutes.BROWSE}`} />;
    }

    return (
        <>
            <Affix offsetTop={60}>
                <LegacyBrowsePath type={entityType} path={path} isBrowsable />
            </Affix>
            {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
            {data && data.browse && !loading && (
                <BrowseResults
                    type={entityType}
                    rootPath={rootPath}
                    title={path.length > 0 ? path[path.length - 1] : entityRegistry.getCollectionName(entityType)}
                    page={page}
                    pageSize={BrowseCfg.RESULTS_PER_PAGE}
                    groups={data.browse.groups}
                    entities={data.browse.entities}
                    totalResults={data.browse.total}
                    onChangePage={onChangePage}
                />
            )}
        </>
    );
};
