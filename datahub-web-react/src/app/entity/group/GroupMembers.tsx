import { CloseOutlined } from '@ant-design/icons';
import { Button, List, message, Modal, Pagination, Row, Space, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { useGetGroupMembersLazyQuery, useRemoveGroupMembersMutation } from '../../../graphql/group.generated';
import { CorpUser, EntityRelationshipsResult, EntityType } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { PreviewType } from '../Entity';
import { AddGroupMembersModal } from './AddGroupMembersModal';

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

const HeaderView = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

export default function GroupMembers({ urn, initialRelationships, pageSize }: Props) {
    const entityRegistry = useEntityRegistry();

    const [page, setPage] = useState(1);
    const [isEditingMembers, setIsEditingMembers] = useState(false);
    const [getMembers, { data: membersData }] = useGetGroupMembersLazyQuery();
    const [removeGroupMembersMutation] = useRemoveGroupMembersMutation();

    const onChangeMembersPage = (newPage: number) => {
        setPage(newPage);
        const start = (newPage - 1) * pageSize;
        getMembers({ variables: { urn, start, count: pageSize } });
    };

    const removeGroupMember = (userUrn: string) => {
        removeGroupMembersMutation({
            variables: {
                groupUrn: urn,
                userUrns: [userUrn],
            },
        })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to remove group member!: \n ${e.message || ''}`, duration: 3 });
            })
            .finally(() => {
                message.success({
                    content: `Removed group member!`,
                    duration: 3,
                });
                // Hack to deal with eventual consistency
                setTimeout(function () {
                    // Reload the page.
                    onChangeMembersPage(page);
                }, 2000);
            });
    };

    const onClickEditMembers = () => {
        setIsEditingMembers(true);
    };

    const onAddMembers = () => {
        setTimeout(function () {
            // Reload the page.
            onChangeMembersPage(page);
        }, 3000);
    };

    const onRemoveMember = (memberUrn: string) => {
        Modal.confirm({
            title: `Confirm Group Member Removal`,
            content: `Are you sure you want to remove this user from the group?`,
            onOk() {
                removeGroupMember(memberUrn);
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const relationships = membersData ? membersData.corpGroup?.relationships : initialRelationships;
    const total = relationships?.total || 0;
    const groupMembers = relationships?.relationships?.map((rel) => rel.entity as CorpUser) || [];

    return (
        <MembersView direction="vertical" size="middle">
            <HeaderView>
                <Typography.Title level={3}>Group Membership</Typography.Title>
                <Button onClick={onClickEditMembers}>+ Add Members</Button>
            </HeaderView>
            <Row justify="center">
                <MemberList
                    dataSource={groupMembers}
                    split={false}
                    renderItem={(item: any, _) => (
                        <List.Item>
                            {entityRegistry.renderPreview(EntityType.CorpUser, PreviewType.PREVIEW, item)}
                            <CloseOutlined onClick={() => onRemoveMember(item.urn)} />
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
            <AddGroupMembersModal
                urn={urn}
                visible={isEditingMembers}
                onSubmit={onAddMembers}
                onClose={() => setIsEditingMembers(false)}
            />
        </MembersView>
    );
}
