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
    // padding: 0 10px;
    height: calc(100vh - 114px);
    overflow-y: auto;
`;
const GroupItemColumn = styled(Col)`
    // border: 1px solid red;
    padding: 10px;
`;
const GroupItem = styled.div`
    border: 1px solid #d9d9d9;
    padding: 10px;
    min-height: 107px;
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
`;
const DummyGroupDescriptionText =
    'Some text used for checking the content of the group item box. Some more text is added here.';
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
                                <GroupItemColumn xl={8} lg={8} md={8} sm={12} xs={24}>
                                    <Link to={entityRegistry.getEntityUrl(EntityType.CorpGroup, item.urn)}>
                                        <GroupItem>
                                            <Row style={{ padding: '9px 11px 9px 11px' }}>
                                                <GroupTitle>{item.name}</GroupTitle>
                                                <GroupMember>10 members</GroupMember>
                                            </Row>
                                            <Row style={{ padding: '2px 13px' }}>
                                                <GroupDescription>
                                                    {item.info?.description && item.info?.description.length > 85 ? (
                                                        <Tooltip title={item.info?.description}>
                                                            {`${item.info?.description.slice(0, 80)} ...`}
                                                        </Tooltip>
                                                    ) : (
                                                        item.info?.description
                                                    )}
                                                </GroupDescription>
                                            </Row>
                                        </GroupItem>
                                    </Link>
                                </GroupItemColumn>
                            );
                        })}
                    {/* dummy */}
                    <GroupItemColumn xl={8} lg={8} md={8} sm={12} xs={24}>
                        <GroupItem>
                            <Row style={{ padding: '9px 11px 9px 11px' }}>
                                <GroupTitle>Some title</GroupTitle>
                                <GroupMember>10 members</GroupMember>
                            </Row>
                            <Row style={{ padding: '2px 13px' }}>
                                <GroupDescription>
                                    {DummyGroupDescriptionText.length > 85 ? (
                                        <Tooltip title={DummyGroupDescriptionText}>
                                            {`${DummyGroupDescriptionText.slice(0, 80)}...`}
                                        </Tooltip>
                                    ) : (
                                        DummyGroupDescriptionText
                                    )}
                                </GroupDescription>
                            </Row>
                        </GroupItem>
                    </GroupItemColumn>
                    <GroupItemColumn xl={8} lg={8} md={8} sm={12} xs={24}>
                        <GroupItem>
                            <Row style={{ padding: '9px 11px 9px 11px' }}>
                                <GroupTitle>Some title</GroupTitle>
                                <GroupMember>10 members</GroupMember>
                            </Row>
                            <Row style={{ padding: '2px 13px' }}>
                                <GroupDescription>
                                    Some text used for checking the content of the group item box.
                                </GroupDescription>
                            </Row>
                        </GroupItem>
                    </GroupItemColumn>
                    <GroupItemColumn xl={8} lg={8} md={8} sm={12} xs={24}>
                        <GroupItem>
                            <Row style={{ padding: '9px 11px 9px 11px' }}>
                                <GroupTitle>Some title</GroupTitle>
                                <GroupMember>10 members</GroupMember>
                            </Row>
                            <Row style={{ padding: '2px 13px' }}>
                                <GroupDescription>
                                    Some text used for checking the content of the group item box.
                                </GroupDescription>
                            </Row>
                        </GroupItem>
                    </GroupItemColumn>
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
