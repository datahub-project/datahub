import * as React from 'react';
import { useHistory, useLocation } from 'react-router';
import * as QueryString from 'query-string';
import { Affix } from 'antd';
import { EntityType, fromPathName, toCollectionName, toPathName } from '../shared/EntityTypeUtil';
import { BrowseCfg } from '../../conf';
import { BrowseResults, BrowseResult } from './BrowseResults';
import { PageRoutes } from '../../conf/Global';
import { SearchablePage } from '../search/SearchablePage';
import { useGetBrowseResultsQuery } from '../../graphql/browse.generated';
import { BrowsePath } from './BrowsePath';
import { BrowseResultEntity, BrowseResultGroup, BrowseResults as BrowseResultsData } from '../../types.generated';

const { RESULTS_PER_PAGE } = BrowseCfg;

export const BrowseResultsPage = () => {
    const location = useLocation();
    const history = useHistory();

    const params = QueryString.parse(location.search);
    const type = fromPathName(params.type as string);
    const path = params.path ? (params.path as string).split('/') : [];
    const page = params.page && Number(params.page as string) > 0 ? Number(params.page as string) : 1;

    const { data, loading, error } = useGetBrowseResultsQuery({
        variables: {
            input: {
                type,
                path,
                start: (page - 1) * RESULTS_PER_PAGE,
                count: RESULTS_PER_PAGE,
                filters: null,
            },
        },
    });

    const toUrlPath = (origPath: Array<string>) => {
        return origPath.join('/');
    };

    const onSelectPath = (part: string) => {
        const newPath = [...path, part];
        history.push({
            pathname: PageRoutes.BROWSE,
            search: `?type=${toPathName(type)}&path=${encodeURIComponent(toUrlPath(newPath))}`,
        });
    };

    const onChangePage = (newPage: number) => {
        history.push({
            pathname: PageRoutes.BROWSE,
            search: `?type=${toPathName(type)}&path=${encodeURIComponent(toUrlPath(path))}&page=${newPage}`,
        });
    };

    const navigateToDataset = (urn: string) => {
        return history.push({
            pathname: `${PageRoutes.DATASETS}/${urn}`,
        });
    };

    const getOnNavigate = (urn: string) => {
        switch (type) {
            case EntityType.Dataset:
                return () => navigateToDataset(urn);
            default:
                throw new Error(`Unrecognized entity with type ${type} provided`);
        }
    };

    const getBrowseEntityResults = (entities: Array<BrowseResultEntity>): Array<BrowseResult> => {
        return entities.map((entity) => ({
            name: entity.name,
            onNavigate: getOnNavigate(entity.urn),
        }));
    };

    const getBrowseGroupResults = (groups: Array<BrowseResultGroup>): Array<BrowseResult> => {
        return groups.map((group) => ({
            name: group.name,
            count: group.count,
            onNavigate: () => onSelectPath(group.name),
        }));
    };

    const getBrowseResults = (browseResults: BrowseResultsData): Array<BrowseResult> => {
        if (browseResults.metadata.groups.length > 0) {
            return getBrowseGroupResults(browseResults.metadata.groups);
        }
        return getBrowseEntityResults(browseResults.entities);
    };
    return (
        <SearchablePage>
            <Affix offsetTop={64}>
                <BrowsePath type={type} path={path} />
            </Affix>
            {error && <p>Error fetching browse results!</p>}
            {loading && <p>Loading browse results...</p>}
            {data && data.browse && (
                <BrowseResults
                    title={path.length > 0 ? path[path.length - 1] : toCollectionName(type)}
                    pageSize={RESULTS_PER_PAGE}
                    pageStart={page * RESULTS_PER_PAGE}
                    results={getBrowseResults(data.browse)}
                    totalResults={data.browse.total}
                    onChangePage={onChangePage}
                />
            )}
        </SearchablePage>
    );
};
