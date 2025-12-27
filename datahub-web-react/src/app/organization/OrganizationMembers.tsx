import { MoreOutlined, UserAddOutlined, UserDeleteOutlined } from '@ant-design/icons';
import { Button, Col, Dropdown, Empty, MenuProps, Modal, Pagination, Row, Typography, message } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { CustomAvatar } from '@app/shared/avatar';
import { scrollToTop } from '@app/shared/searchUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetEntitiesByOrganizationQuery, useRemoveUserFromOrganizationsMutation } from '../../graphql/organization.generated';
import { CorpUser, EntityType } from '../../types.generated';
import { AddOrganizationMembersModal } from './AddOrganizationMembersModal';

const ADD_MEMBER_STYLE = {
    backGround: '#ffffff',
    boxShadow: '0px 2px 6px rgba(0, 0, 0, 0.05)',
};
const AVATAR_STYLE = { margin: '5px 5px 5px 0' };

/**
 * Styled Components
 */
const AddMember = styled(Button)`
    padding: 13px 13px 30px 30px;
    cursor: pointer;

    &&& .anticon.anticon-user-add {
        margin-right: 6px;
    }
`;

const AddMemberText = styled(Typography.Text)`
    font-family: Mulish;
    font-style: normal;
    font-weight: 500;
    font-size: 12px;
    line-height: 20px;
`;

const MemberNameSection = styled.div`
    font-size: 20px;
    line-height: 28px;
    color: #262626;
    display: flex;
    align-items: center;
    justify-content: start;
    padding-left: 12px;
`;

const GroupMemberWrapper = styled.div`
    height: calc(100vh - 217px);
    overflow-y: auto;

    & .groupMemberRow {
        margin: 0 19px;
    }
`;

const MemberColumn = styled(Col)`
    padding: 19px 0 19px 0;
    border-bottom: 1px solid #f0f0f0;
`;

const MemberEditIcon = styled.div`
    font-size: 22px;
    float: right;
`;

const Name = styled.span`
    font-weight: bold;
    font-size: 14px;
    line-height: 22px;
    color: #262626;
    margin-left: 8px;
`;

const NoGroupMembers = styled(Empty)`
    padding: 40px;
`;

const StyledMoreOutlined = styled(MoreOutlined)`
    :hover {
        cursor: pointer;
    }
`;

import { useEntityContext } from '@app/entity/shared/EntityContext';

// ...

type Props = {
    urn?: string;
    pageSize?: number;
    onChangeMembers?: () => void;
};

export const OrganizationMembers = ({ urn: propUrn, pageSize = 20, onChangeMembers }: Props) => {
    const { urn: contextUrn } = useEntityContext();
    const urn = propUrn || contextUrn;
    const entityRegistry = useEntityRegistry();

    const [page, setPage] = useState(1);
    const [isEditingMembers, setIsEditingMembers] = useState(false);
    const start = (page - 1) * pageSize;

    const { data: membersData, refetch } = useGetEntitiesByOrganizationQuery({
        variables: {
            organizationUrn: urn,
            entityTypes: [EntityType.CorpUser],
            start,
            count: pageSize
        },
        fetchPolicy: 'cache-first',
    });

    const [removeUserFromOrganizationsMutation] = useRemoveUserFromOrganizationsMutation();

    const onChangeMembersPage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    const removeMember = (userUrn: string) => {
        removeUserFromOrganizationsMutation({
            variables: {
                userUrn: userUrn,
                organizationUrns: [urn],
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({ content: 'Removed Member!', duration: 2 });
                    // Hack to deal with eventual consistency
                    setTimeout(() => {
                        // Reload the page.
                        refetch();
                    }, 3000);
                    onChangeMembers?.();
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to remove member: \n ${e.message || ''}`, duration: 3 });
            });
    };

    const onClickEditMembers = () => {
        setIsEditingMembers(true);
    };

    const onAddMembers = () => {
        setTimeout(() => {
            refetch();
        }, 3000);
        onChangeMembers?.();
    };

    const onRemoveMember = (memberUrn: string) => {
        Modal.confirm({
            title: `Confirm Member Removal`,
            content: `Are you sure you want to remove this user from the organization?`,
            onOk() {
                removeMember(memberUrn);
            },
            onCancel() { },
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const total = membersData?.getEntitiesByOrganization?.total || 0;
    const members = membersData?.getEntitiesByOrganization?.entities?.map((entity) => entity as CorpUser) || [];

    const getItems = (urnID: string): MenuProps['items'] => {
        return [
            {
                key: 'remove',
                onClick: () => onRemoveMember(urnID),
                label: (
                    <span>
                        <UserDeleteOutlined /> Remove from Organization
                    </span>
                ),
            },
        ];
    };

    return (
        <>
            <Row style={ADD_MEMBER_STYLE}>
                <AddMember
                    type="text"
                    onClick={onClickEditMembers}
                    data-testid="add-organization-member-button"
                >
                    <UserAddOutlined />
                    <AddMemberText>Add Member</AddMemberText>
                </AddMember>
            </Row>
            <GroupMemberWrapper>
                {members.length === 0 && <NoGroupMembers description="No members in this organization yet." />}
                {members
                    ? members.map((item) => {
                        const entityUrn = entityRegistry.getEntityUrl(EntityType.CorpUser, item.urn);
                        return (
                            <Row className="groupMemberRow" align="middle" key={entityUrn}>
                                <MemberColumn xl={23} lg={23} md={23} sm={23} xs={23}>
                                    <Link to={entityUrn}>
                                        <MemberNameSection>
                                            <CustomAvatar
                                                useDefaultAvatar={false}
                                                size={28}
                                                photoUrl={item.editableProperties?.pictureLink || ''}
                                                name={entityRegistry.getDisplayName(EntityType.CorpUser, item)}
                                                style={AVATAR_STYLE}
                                            />
                                            <Name>{entityRegistry.getDisplayName(EntityType.CorpUser, item)}</Name>
                                        </MemberNameSection>
                                    </Link>
                                </MemberColumn>
                                <MemberColumn xl={1} lg={1} md={1} sm={1} xs={1}>
                                    <MemberEditIcon>
                                        <Dropdown menu={{ items: getItems(item.urn) }}>
                                            <StyledMoreOutlined />
                                        </Dropdown>
                                    </MemberEditIcon>
                                </MemberColumn>
                            </Row>
                        );
                    })
                    : null}
            </GroupMemberWrapper>
            <Row justify="center" style={{ marginTop: '15px' }}>
                <Pagination
                    current={page}
                    pageSize={pageSize}
                    total={total}
                    showLessItems
                    onChange={onChangeMembersPage}
                    showSizeChanger={false}
                />
            </Row>
            {isEditingMembers && (
                <AddOrganizationMembersModal
                    urn={urn}
                    visible={isEditingMembers}
                    onSubmit={onAddMembers}
                    onCloseModal={() => setIsEditingMembers(false)}
                />
            )}
        </>
    );
};
