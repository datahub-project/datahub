import { Col, Row, Tooltip } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
// import { useGetUserGroupsLazyQuery } from '../../../graphql/user.generated';
import { CorpGroup, EntityRelationshipsResult, EntityType } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';

// import { Col, List, Pagination, Row, Space, Tooltip, Typography } from 'antd';
// import React, { useState } from 'react';
// import { Link } from 'react-router-dom';
// import styled from 'styled-components';
// import { useGetUserGroupsLazyQuery } from '../../../graphql/user.generated';
// import { CorpGroup, EntityRelationshipsResult, EntityType } from '../../../types.generated';
// import { useEntityRegistry } from '../../useEntityRegistry';
// import { PreviewType } from '../Entity';

type Props = {
    urn: string;
    initialRelationships?: EntityRelationshipsResult | null;
    pageSize: number;
};

// const GroupList = styled(List)`
//     &&& {
//         width: 100%;
//         border-color: ${(props) => props.theme.styles['border-color-base']};
//         margin-top: 12px;
//         margin-bottom: 28px;
//         padding: 24px 32px;
//         box-shadow: ${(props) => props.theme.styles['box-shadow']};
//     }
//     & li {
//         padding-top: 28px;
//         padding-bottom: 28px;
//     }
//     & li:not(:last-child) {
//         border-bottom: 1.5px solid #ededed;
//     }
// `;

// const GroupsView = styled(Space)`
//     width: 100%;
//     margin-bottom: 32px;
//     padding-top: 28px;
// `;

// New design CSS
const GroupsViewWrapper = styled.div`
    height: calc(100vh - 114px);
    overflow-y: auto;
`;
const GroupItemColumn = styled(Col)`
    padding: 10px;
`;
const GroupItem = styled.div`
    border: 1px solid #d9d9d9;
    padding: 10px;
    min-height: 107px;
    max-height: 107px;

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
    // const [page, setPage] = useState(1);
    const entityRegistry = useEntityRegistry();

    // const [getGroups, { data: groupsData }] = useGetUserGroupsLazyQuery();

    // const onChangeGroupsPage = (newPage: number) => {
    //     setPage(newPage);
    //     const start = (newPage - 1) * pageSize;
    //     getGroups({ variables: { urn, start, count: pageSize } });
    // };

    // const relationships = groupsData ? groupsData.corpUser?.relationships : initialRelationships;
    const relationships = initialRelationships;
    // const total = relationships?.total || 0;
    const userGroups = relationships?.relationships?.map((rel) => rel.entity as CorpGroup) || [];
    console.log('group', userGroups, urn, pageSize);

    return (
        <>
            <GroupsViewWrapper>
                <Row justify="space-between">
                    {userGroups &&
                        userGroups.map((item) => {
                            return (
                                <GroupItemColumn xl={8} lg={8} md={12} sm={12} xs={24}>
                                    <Link to={entityRegistry.getEntityUrl(EntityType.CorpGroup, item.urn)}>
                                        <GroupItem>
                                            <Row className="title-row">
                                                <GroupTitle>{item.name}</GroupTitle>
                                                <GroupMember>
                                                    {item.relationships?.total}
                                                    {item.relationships?.total === 1 ? ' member' : ' members'}
                                                </GroupMember>
                                            </Row>
                                            <Row className="description-row">
                                                <GroupDescription>
                                                    <Tooltip title={item.info?.description}>
                                                        {`${item.info?.description}`}
                                                    </Tooltip>
                                                </GroupDescription>
                                            </Row>
                                        </GroupItem>
                                    </Link>
                                </GroupItemColumn>
                            );
                        })}
                    {/* dummy */}
                    {/* dummy end */}
                </Row>
            </GroupsViewWrapper>
            {/* <GroupsView direction="vertical" size="middle">
                <Typography.Title level={3}>Group Membership</Typography.Title>
                <Row justify="center">
                    <GroupList
                        dataSource={userGroups}
                        split={false}
                        renderItem={(item, _) => (
                            <List.Item>
                                {entityRegistry.renderPreview(EntityType.CorpGroup, PreviewType.PREVIEW, item)}
                            </List.Item>
                        )}
                        bordered
                    />
                    <Pagination
                        current={page}
                        pageSize={pageSize}
                        total={total}
                        showLessItems
                        onChange={onChangeGroupsPage}
                        showSizeChanger={false}
                    />
                </Row>
            </GroupsView> */}
        </>
    );
}
