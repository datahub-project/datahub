import { Pagination, Row, Space } from 'antd';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';
import { EntityType, SearchResult } from '../../../types.generated';
import { useGetAllEntitySearchResults } from '../../../utils/customGraphQL/useGetAllEntitySearchResults';
import RelatedEntityResults from '../../shared/entitySearch/RelatedEntityResults';
import { useEntityRegistry } from '../../useEntityRegistry';
import { GetGroupQuery } from '../../../graphql/group.generated';

type Props = {
    data?: GetGroupQuery;
    pageSize: number;
};

const OwnershipView = styled(Space)`
    width: 100%;
    margin-bottom: 32px;
    padding-top: 28px;
`;

export default function GroupOwnerships({ data, pageSize }: Props) {
    const [page, setPage] = useState(1);
    const [currentMenuKey, setCurrentMenuKey] = useState('');
    const entityRegistry = useEntityRegistry();

    const ownershipResult = useGetAllEntitySearchResults({
        query: `owners:${data?.corpGroup?.name}`,
        start: (page - 1) * pageSize,
        count: pageSize,
    });

    const onChangePage = (newPage: number) => {
        setPage(newPage);
    };

    const setCurrentMenuKeyHook = (menuKey: string) => {
        setPage(1);
        setCurrentMenuKey(menuKey);
    };

    const [ownershipForDetails, totalResultsLength] = useMemo(() => {
        let total = 0;
        const filteredOwnershipResult: {
            [key in EntityType]?: Array<SearchResult>;
        } = {};

        Object.keys(ownershipResult).forEach((type) => {
            const search = ownershipResult[type].data?.search;
            if (search && search.total > 0) {
                filteredOwnershipResult[type] = [];
            }
            if (currentMenuKey === entityRegistry.getPathName(type as EntityType)) {
                total = ownershipResult[type].data?.search?.total;

                const selectedTabPaginatedResults = ownershipResult[type].data?.search?.searchResults;
                if (selectedTabPaginatedResults && selectedTabPaginatedResults.length > 0) {
                    filteredOwnershipResult[type] = selectedTabPaginatedResults;
                }
            }
        });
        return [filteredOwnershipResult, total];
    }, [ownershipResult, entityRegistry, currentMenuKey]);

    return (
        <OwnershipView direction="vertical" size="middle">
            <Row justify="center">
                <RelatedEntityResults searchResult={ownershipForDetails} menuItemChangeHook={setCurrentMenuKeyHook} />
                <Pagination
                    current={page}
                    pageSize={pageSize}
                    total={totalResultsLength}
                    showLessItems
                    onChange={onChangePage}
                    showSizeChanger={false}
                />
            </Row>
        </OwnershipView>
    );
}
