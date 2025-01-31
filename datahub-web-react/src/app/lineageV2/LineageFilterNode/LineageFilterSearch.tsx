import { LoadingOutlined } from '@ant-design/icons';
import { DBT_URN } from '@app/ingest/source/builder/constants';
import { useGetLineageTimeParams } from '@app/lineage/utils/useGetLineageTimeParams';
import { LineageFilter, LineageNodesContext, useIgnoreSchemaFieldStatus } from '@app/lineageV2/common';
import computeOrFilters from '@app/lineageV2/LineageFilterNode/computeOrFilters';
import { DEGREE_FILTER_NAME } from '@app/search/utils/constants';
import { Input, Text } from '@components';
import { useSearchAcrossLineageNamesQuery } from '@graphql/lineage.generated';
import { EntityType } from '@types';
import { Spin } from 'antd';
import React, { useContext, useEffect, useState } from 'react';
import { usePrevious } from 'react-js-cron/dist/cjs/utils';
import { useDebounce } from 'react-use';
import styled from 'styled-components';

const SearchLine = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    margin-top: 2px;
`;

const SearchInput = styled(Input)`
    height: 2em;
`;

const LoadingWrapper = styled.div`
    min-width: 20px;
`;

const SearchMatchesText = styled(Text)``;

interface Props {
    data: LineageFilter;
    numMatches: number;
    setNumMatches: (numMatches: number) => void;
}

export default function LineageFilterSearch({ data, numMatches, setNumMatches }: Props) {
    const { id, direction, parent, limit } = data;

    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();
    const { nodes, setDisplayVersion, rootType, showGhostEntities } = useContext(LineageNodesContext);
    const ignoreSchemaFieldStatus = useIgnoreSchemaFieldStatus();

    const [inputValue, setInputValue] = useState('');
    const [searchQuery, setSearchQuery] = useState('');
    const oldSearchQuery = usePrevious(searchQuery);

    useDebounce(() => setSearchQuery(inputValue), 300, [inputValue]);
    useEffect(() => {
        const filters = nodes.get(parent)?.filters[direction];
        if (filters && searchQuery.length < 3 && oldSearchQuery?.length >= 3) {
            filters.searchUrns = undefined;
            setNumMatches(0);
            setDisplayVersion(([prev]) => [prev + 1, [id]]);
        }
    }, [searchQuery, oldSearchQuery, id, parent, direction, nodes, setNumMatches, setDisplayVersion]);

    const orFilters = computeOrFilters([{ field: DEGREE_FILTER_NAME, values: ['1'] }]);
    const { loading } = useSearchAcrossLineageNamesQuery({
        skip: searchQuery.length < 3,
        variables: {
            input: {
                query: searchQuery,
                urn: parent,
                direction,
                count: searchQuery ? limit : 0,
                orFilters,
                lineageFlags: {
                    startTimeMillis,
                    endTimeMillis,
                    ignoreAsHops: [
                        {
                            entityType: EntityType.Dataset,
                            platforms: [DBT_URN],
                        },
                        { entityType: EntityType.DataJob },
                    ],
                },
                searchFlags: {
                    includeSoftDeleted:
                        showGhostEntities || (rootType === EntityType.SchemaField && ignoreSchemaFieldStatus),
                },
            },
        },
        onCompleted: (queryData) => {
            setNumMatches(queryData.searchAcrossLineage?.total || 0);
            const filters = nodes.get(parent)?.filters[direction];
            if (filters) {
                filters.searchUrns = new Set(
                    queryData.searchAcrossLineage?.searchResults?.map((result) => result.entity.urn),
                );
                setDisplayVersion(([prev]) => [prev + 1, [id]]);
            }
        },
    });

    return (
        <>
            <SearchLine>
                <SearchInput
                    label=""
                    placeholder="Search children"
                    error={!!inputValue && inputValue.length < 3 ? 'Minimum 3 characters required' : undefined}
                    errorOnHover
                    value={inputValue}
                    setValue={setInputValue}
                />
                <LoadingWrapper>{loading && <Spin indicator={<LoadingOutlined />} />}</LoadingWrapper>
            </SearchLine>
            <SearchMatchesText type="div" size="xs" color="gray">
                {searchQuery.length >= 3 && (!loading || !!numMatches) && `${numMatches} matches`}
            </SearchMatchesText>
        </>
    );
}
