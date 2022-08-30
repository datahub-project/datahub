import { Col, Pagination, Row, Tooltip } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { useGetUserGroupsLazyQuery } from '../../../graphql/user.generated';
import { CorpGroup, EntityRelationshipsResult, EntityType } from '../../../types.generated';
import { scrollToTop } from '../../shared/searchUtils';
import { useEntityRegistry } from '../../useEntityRegistry';

type Props = {
    urn: string;
    initialRelationships?: EntityRelationshipsResult | null;
    pageSize: number;
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

const GroupItemColumn = styled(Col)`
    padding: 10px;
`;

const GroupItem = styled.div`
    border: 1px solid #eaeaea;
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
`;

const GroupTitle = styled.span`
    font-size: 14px;
    line-height: 22px;
    font-weight: bold;
    color: #262626;
`;

const GroupMember = styled.span`
    font-weight: 500;
    font-size: 12px;
    line-height: 23px;
    color: #8c8c8c;
    padding-left: 7px;
`;

const GroupDescription = styled.span`
    font-weight: 500;
    font-size: 12px;
    line-height: 20px;
    color: #262626;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 100%;
    height: 43px;
`;
export default function UserGroups({ urn, initialRelationships, pageSize }: Props) {
    const [page, setPage] = useState(1);
    const entityRegistry = useEntityRegistry();

    const [getGroups, { data: groupsData }] = useGetUserGroupsLazyQuery();

    const onChangeGroupsPage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
        const start = (newPage - 1) * pageSize;
        getGroups({ variables: { urn, start, count: pageSize } });
    };

    const relationships = groupsData ? groupsData.corpUser?.relationships : initialRelationships;
    const total = relationships?.total || 0;
    const userGroups = relationships?.relationships?.map((rel) => rel.entity as CorpGroup) || [];

    return (
        <GroupsViewWrapper>
            <Row justify="start">
                {userGroups &&
                    userGroups.map((item) => {
                        return (
                            <GroupItemColumn xl={8} lg={8} md={12} sm={12} xs={24} key={item.urn}>
                                <Link to={entityRegistry.getEntityUrl(EntityType.CorpGroup, item.urn)}>
                                    <GroupItem>
                                        <Row className="title-row">
                                            <GroupTitle>{item.info?.displayName || item.name}</GroupTitle>
                                            <GroupMember>
                                                {item.relationships?.total}
                                                {item.relationships?.total === 1 ? ' member' : ' members'}
                                            </GroupMember>
                                        </Row>
                                        <Row className="description-row">
                                            <GroupDescription>
                                                <Tooltip title={item.info?.description}>
                                                    {item.info?.description}
                                                </Tooltip>
                                            </GroupDescription>
                                        </Row>
                                    </GroupItem>
                                </Link>
                            </GroupItemColumn>
                        );
                    })}
            </Row>
            <Row className="user-group-pagination">
                <Pagination
                    current={page}
                    pageSize={pageSize}
                    total={total}
                    showLessItems
                    onChange={onChangeGroupsPage}
                    showSizeChanger={false}
                />
            </Row>
        </GroupsViewWrapper>
    );
}
