import { List, Pagination, Row, Space, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { useGetGroupMembersLazyQuery } from '../../../graphql/group.generated';
import { CorpUser, EntityRelationshipsResult, EntityType } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { PreviewType } from '../Entity';

type Props = {
    urn: string;
    initialRelationships?: EntityRelationshipsResult | null;
    pageSize: number;
};

const MemberList = styled(List)`
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

const MembersView = styled(Space)`
    width: 100%;
    margin-bottom: 32px;
    padding-top: 28px;
`;

export default function GroupMembers({ urn, initialRelationships, pageSize }: Props) {
    const [page, setPage] = useState(1);
    const entityRegistry = useEntityRegistry();

    const [getMembers, { data: membersData }] = useGetGroupMembersLazyQuery();

    const onChangeMembersPage = (newPage: number) => {
        setPage(newPage);
        const start = (newPage - 1) * pageSize;
        getMembers({ variables: { urn, start, count: pageSize } });
    };

    const relationships = membersData ? membersData.corpGroup?.relationships : initialRelationships;
    const total = relationships?.total || 0;
    const groupMembers = relationships?.relationships?.map((rel) => rel.entity as CorpUser) || [];

    return (
        <MembersView direction="vertical" size="middle">
            <Typography.Title level={3}>Group Membership</Typography.Title>
            <Row justify="center">
                <MemberList
                    dataSource={groupMembers}
                    split={false}
                    renderItem={(item, _) => (
                        <List.Item>
                            {entityRegistry.renderPreview(EntityType.CorpUser, PreviewType.PREVIEW, item)}
                        </List.Item>
                    )}
                    bordered
                />
                <Pagination
                    current={page}
                    pageSize={pageSize}
                    total={total}
                    showLessItems
                    onChange={onChangeMembersPage}
                    showSizeChanger={false}
                />
            </Row>
        </MembersView>
    );
}
