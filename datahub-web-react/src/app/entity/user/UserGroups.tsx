import { List, Pagination, Row, Space, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { useGetUserGroupsLazyQuery } from '../../../graphql/user.generated';
import { CorpGroup, EntityRelationshipsResult, EntityType } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { PreviewType } from '../Entity';

type Props = {
    urn: string;
    initialRelationships?: EntityRelationshipsResult | null;
    pageSize: number;
};

const GroupList = styled(List)`
    &&& {
        width: 100%;
        border-color: ${(props) => props.theme.styles['border-color-base']};
        margin-top: 12px;
        margin-bottom: 28px;
        padding: 24px 32px;
        box-shadow: ${(props) => props.theme.styles['box-shadow']};
    }
    & li {
        padding-top: 28px;
        padding-bottom: 28px;
    }
    & li:not(:last-child) {
        border-bottom: 1.5px solid #ededed;
    }
`;

const GroupsView = styled(Space)`
    width: 100%;
    margin-bottom: 32px;
    padding-top: 28px;
`;

export default function UserGroups({ urn, initialRelationships, pageSize }: Props) {
    const [page, setPage] = useState(1);
    const entityRegistry = useEntityRegistry();

    const [getGroups, { data: groupsData }] = useGetUserGroupsLazyQuery();

    const onChangeGroupsPage = (newPage: number) => {
        setPage(newPage);
        const start = (newPage - 1) * pageSize;
        getGroups({ variables: { urn, start, count: pageSize } });
    };

    const relationships = groupsData ? groupsData.corpUser?.relationships : initialRelationships;
    const total = relationships?.total || 0;
    const userGroups = relationships?.relationships?.map((rel) => rel.entity as CorpGroup) || [];

    return (
        <GroupsView direction="vertical" size="middle">
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
        </GroupsView>
    );
}
