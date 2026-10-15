import { Pagination, Tooltip } from '@components';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { scrollToTop } from '@app/shared/searchUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetUserGroupsLazyQuery } from '@graphql/user.generated';
import { CorpGroup, EntityRelationship, EntityType } from '@types';

type Props = {
    urn: string;
    initialRelationships?: Array<EntityRelationship> | null;
    pageSize: number;
    totalRelationships: number;
};

const GroupsViewWrapper = styled.div`
    height: calc(100vh - 173px);
    overflow-y: auto;

    .user-group-pagination {
        justify-content: center;
        bottom: 24px;
        position: absolute;
        width: 100%;
        left: 50%;
        -webkit-transform: translateX(-50%);
        -moz-transform: translateX(-50%);
        -webkit-transform: translateX(-50%);
        -ms-transform: translateX(-50%);
        transform: translateX(-50%);
    }
`;

const GroupsGrid = styled.div`
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));

    @media (max-width: 992px) {
        grid-template-columns: repeat(2, minmax(0, 1fr));
    }

    @media (max-width: 576px) {
        grid-template-columns: minmax(0, 1fr);
    }
`;

const GroupItemColumn = styled.div`
    padding: 10px;
`;

const GroupRow = styled.div`
    display: flex;
    align-items: center;
`;

const PaginationRow = styled.div`
    display: flex;
`;

const GroupItem = styled.div`
    border: 1px solid ${(props) => props.theme.colors.border};
    padding: 10px;
    min-height: 107px;
    max-height: 107px;
    border-radius: 5px;

    .title-row {
        padding: 9px 11px 9px 11px;
    }
    .description-row {
        padding: 2px 13px;
    }
    overflow: hidden;
`;

const GroupTitle = styled.span`
    font-size: 14px;
    line-height: 22px;
    font-weight: bold;
    color: ${(props) => props.theme.colors.text};
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
`;

const GroupMember = styled.span`
    font-weight: 500;
    font-size: 12px;
    line-height: 23px;
    color: ${(props) => props.theme.colors.textSecondary};
    padding-left: 7px;
`;

const GroupDescription = styled.span`
    font-weight: 500;
    font-size: 12px;
    line-height: 20px;
    color: ${(props) => props.theme.colors.text};
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 100%;
`;

export default function UserGroups({ urn, initialRelationships, pageSize, totalRelationships }: Props) {
    const { t } = useTranslation('entity.types');
    const [page, setPage] = useState(1);
    const entityRegistry = useEntityRegistry();

    const [getGroups, { data: groupsData }] = useGetUserGroupsLazyQuery();

    const onChangeGroupsPage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
        const start = (newPage - 1) * pageSize;
        getGroups({ variables: { urn, start, count: pageSize } });
    };

    const relationships = groupsData ? groupsData.corpUser?.relationships?.relationships : initialRelationships;
    const userGroups = [...(relationships || [])].map((rel) => rel.entity as CorpGroup);
    return (
        <GroupsViewWrapper>
            <GroupsGrid>
                {userGroups &&
                    userGroups.map((item) => {
                        return (
                            <GroupItemColumn key={item.urn}>
                                <Link to={entityRegistry.getEntityUrl(EntityType.CorpGroup, item.urn)}>
                                    <GroupItem>
                                        <GroupRow className="title-row">
                                            <GroupTitle>{item.info?.displayName || item.name}</GroupTitle>
                                            <GroupMember>
                                                {t('shared.membersCount', { count: item.relationships?.total || 0 })}
                                            </GroupMember>
                                        </GroupRow>
                                        <GroupRow className="description-row">
                                            <GroupDescription>
                                                <Tooltip title={item.info?.description}>
                                                    {item.info?.description}
                                                </Tooltip>
                                            </GroupDescription>
                                        </GroupRow>
                                    </GroupItem>
                                </Link>
                            </GroupItemColumn>
                        );
                    })}
            </GroupsGrid>
            <PaginationRow className="user-group-pagination">
                <Pagination
                    currentPage={page}
                    itemsPerPage={pageSize}
                    total={totalRelationships}
                    showLessItems
                    onPageChange={onChangeGroupsPage}
                    showSizeChanger={false}
                />
            </PaginationRow>
        </GroupsViewWrapper>
    );
}
