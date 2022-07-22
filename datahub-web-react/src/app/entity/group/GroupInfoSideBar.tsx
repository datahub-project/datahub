import { Divider, message, Space, Button, Typography, Row, Col, Tooltip } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { EditOutlined, LockOutlined, MailOutlined, SlackOutlined } from '@ant-design/icons';
import { useHistory, useRouteMatch } from 'react-router-dom';
import { useUpdateCorpGroupPropertiesMutation } from '../../../graphql/group.generated';
import { EntityRelationshipsResult, Ownership } from '../../../types.generated';

import GroupEditModal from './GroupEditModal';
import CustomAvatar from '../../shared/avatar/CustomAvatar';
import GroupOwnerSideBarSection from './GroupOwnerSideBarSection';
import {
    SideBar,
    SideBarSubSection,
    EmptyValue,
    SocialDetails,
    EditButton,
    AboutSection,
    AboutSectionText,
    GroupsSection,
} from '../shared/SidebarStyledComponents';
import GroupMembersSideBarSection from './GroupMembersSideBarSection';

const { Paragraph } = Typography;

type SideBarData = {
    photoUrl: string | undefined;
    avatarName: string | undefined;
    name: string | undefined;
    email: string | undefined;
    slack: string | undefined;
    aboutText: string | undefined;
    groupMemberRelationships: EntityRelationshipsResult;
    groupOwnerShip: Ownership;
    isExternalGroup: boolean;
    externalGroupType: string | undefined;
    urn: string | undefined;
};

type Props = {
    sideBarData: SideBarData;
    refetch: () => Promise<any>;
};

const AVATAR_STYLE = { margin: '3px 5px 3px 0px' };

const TITLES = {
    about: 'About',
    members: 'Members ',
    editGroup: 'Edit Group',
};

const GroupNameHeader = styled(Row)`
    font-size: 20px;
    line-height: 28px;
    color: #262626;
    margin: 16px 16px 8px 8px;
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 100px;
`;

const GroupName = styled.div`
    max-width: 260px;
    word-wrap: break-word;
    width: 140px;
`;

/**
 * Responsible for reading & writing users.
 */
export default function GroupInfoSidebar({ sideBarData, refetch }: Props) {
    const {
        avatarName,
        name,
        aboutText,
        groupMemberRelationships,
        email,
        photoUrl,
        slack,
        urn,
        isExternalGroup,
        externalGroupType,
        groupOwnerShip: ownership,
    } = sideBarData;
    const [updateCorpGroupPropertiesMutation] = useUpdateCorpGroupPropertiesMutation();
    const { url } = useRouteMatch();
    const history = useHistory();

    /* eslint-disable @typescript-eslint/no-unused-vars */
    const [editGroupModal, showEditGroupModal] = useState(false);
    const canEditGroup = true; // TODO; Replace this will fine-grained understanding of user permissions.

    const getEditModalData = {
        urn,
        email,
        slack,
    };

    // About Text save
    const onSaveAboutMe = (inputString) => {
        updateCorpGroupPropertiesMutation({
            variables: {
                urn: urn || '',
                input: {
                    description: inputString,
                },
            },
        })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to Save changes!: \n ${e.message || ''}`, duration: 3 });
            })
            .finally(() => {
                message.success({
                    content: `Changes saved.`,
                    duration: 3,
                });
                refetch();
            });
    };
    return (
        <>
            <SideBar>
                <SideBarSubSection className={canEditGroup ? '' : 'fullView'}>
                    <GroupNameHeader>
                        <Col>
                            <CustomAvatar
                                useDefaultAvatar={false}
                                size={64}
                                photoUrl={photoUrl}
                                name={avatarName}
                                style={AVATAR_STYLE}
                            />
                        </Col>
                        <Col>
                            <GroupName>{name}</GroupName>
                        </Col>
                        <Col>
                            {isExternalGroup && (
                                <Tooltip
                                    title={`Membership for this group cannot be edited in DataHub as it originates from ${externalGroupType}.`}
                                >
                                    <LockOutlined />
                                </Tooltip>
                            )}
                        </Col>
                    </GroupNameHeader>
                    <Divider className="divider-infoSection" />
                    <SocialDetails>
                        <Space>
                            <MailOutlined />
                            {email || <EmptyValue />}
                        </Space>
                    </SocialDetails>
                    <SocialDetails>
                        <Space>
                            <SlackOutlined />
                            {slack || <EmptyValue />}
                        </Space>
                    </SocialDetails>
                    <Divider className="divider-aboutSection" />
                    <AboutSection>
                        {TITLES.about}
                        <AboutSectionText>
                            <Paragraph
                                editable={canEditGroup ? { onChange: onSaveAboutMe } : false}
                                ellipsis={{ rows: 2, expandable: true, symbol: 'Read more' }}
                            >
                                {aboutText || <EmptyValue />}
                            </Paragraph>
                        </AboutSectionText>
                    </AboutSection>
                    <Divider className="divider-groupsSection" />
                    <GroupsSection>
                        <GroupOwnerSideBarSection ownership={ownership} urn={urn || ''} refetch={refetch} />
                    </GroupsSection>
                    <Divider className="divider-groupsSection" />
                    <GroupsSection>
                        <GroupMembersSideBarSection
                            total={groupMemberRelationships?.total || 0}
                            relationships={groupMemberRelationships?.relationships || []}
                            onSeeMore={() => history.push(`${url}/members`)}
                        />
                    </GroupsSection>
                </SideBarSubSection>
                {canEditGroup && (
                    <EditButton>
                        <Button icon={<EditOutlined />} onClick={() => showEditGroupModal(true)}>
                            {TITLES.editGroup}
                        </Button>
                    </EditButton>
                )}
            </SideBar>
            {/* Modal */}
            <GroupEditModal
                visible={editGroupModal}
                onClose={() => showEditGroupModal(false)}
                onSave={() => {
                    refetch();
                }}
                editModalData={getEditModalData}
            />
        </>
    );
}
