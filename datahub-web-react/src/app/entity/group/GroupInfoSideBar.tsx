import { Divider, message, Space, Button, Typography, Tag } from 'antd';
import React, { useState } from 'react';
import { EditOutlined, MailOutlined, SlackOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { useUpdateCorpGroupPropertiesMutation } from '../../../graphql/group.generated';
import { EntityType, EntityRelationshipsResult, Ownership } from '../../../types.generated';

import GroupEditModal from './GroupEditModal';
import CustomAvatar from '../../shared/avatar/CustomAvatar';
import { useEntityRegistry } from '../../useEntityRegistry';
import { useGetAuthenticatedUser } from '../../useGetAuthenticatedUser';
import SidebarOwnerSection from './SidebarOwnerSection';
import {
    SideBar,
    SideBarSubSection,
    EmptyValue,
    Name,
    SocialDetails,
    EditProfileButton,
    AboutSection,
    AboutSectionText,
    GroupsSection,
    TagsSection,
    Tags,
    GroupsSeeMoreText,
} from '../user/UserInfoSideBar';

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
    urn: string | undefined;
};

type Props = {
    sideBarData: SideBarData;
    refetch: () => Promise<any>;
};

const AVATAR_STYLE = { margin: '3px 5px 3px -4px' };

/**
 * Responsible for reading & writing users.
 */
export default function GroupInfoSidebar({ sideBarData, refetch }: Props) {
    const { avatarName, name, aboutText, groupMemberRelationships, email, photoUrl, slack, urn, groupOwnerShip } =
        sideBarData;
    const [updateCorpGroupPropertiesMutation] = useUpdateCorpGroupPropertiesMutation();
    const entityRegistry = useEntityRegistry();

    const [groupSectionExpanded, setGroupSectionExpanded] = useState(false);
    /* eslint-disable @typescript-eslint/no-unused-vars */
    const [editGroupModal, showEditGroupModal] = useState(false);
    const me = useGetAuthenticatedUser();
    const isProfileOwner = me?.corpUser?.urn === urn;

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
                <SideBarSubSection className={isProfileOwner ? '' : 'fullView'}>
                    <Name>
                        <CustomAvatar size={28} photoUrl={photoUrl} name={avatarName} style={AVATAR_STYLE} />
                        {name}
                    </Name>
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
                        About
                        <AboutSectionText>
                            <Paragraph
                                editable={isProfileOwner ? { onChange: onSaveAboutMe } : false}
                                ellipsis={{ rows: 2, expandable: true, symbol: 'Read more' }}
                            >
                                {aboutText || <EmptyValue />}
                            </Paragraph>
                        </AboutSectionText>
                    </AboutSection>
                    <Divider className="divider-groupsSection" />
                    <GroupsSection>
                        <SidebarOwnerSection OwnerData={groupOwnerShip} urn={urn || ''} refetch={refetch} />
                    </GroupsSection>
                    <Divider className="divider-groupsSection" />
                    <GroupsSection>
                        Members
                        <TagsSection>
                            {groupMemberRelationships?.relationships.length === 0 && <EmptyValue />}
                            {!groupSectionExpanded &&
                                groupMemberRelationships?.relationships.slice(0, 2).map((item) => {
                                    return (
                                        <Link to={entityRegistry.getEntityUrl(EntityType.CorpUser, item.entity.urn)}>
                                            <Tags>
                                                <Tag>
                                                    <CustomAvatar
                                                        size={20}
                                                        photoUrl={photoUrl}
                                                        name={entityRegistry.getDisplayName(
                                                            EntityType.CorpGroup,
                                                            item.entity,
                                                        )}
                                                        style={AVATAR_STYLE}
                                                    />
                                                    {entityRegistry.getDisplayName(EntityType.CorpUser, item.entity)}
                                                </Tag>
                                            </Tags>
                                        </Link>
                                    );
                                })}
                            {groupSectionExpanded &&
                                groupMemberRelationships?.relationships.length > 2 &&
                                groupMemberRelationships?.relationships.map((item) => {
                                    return (
                                        <Link to={entityRegistry.getEntityUrl(EntityType.CorpUser, item.entity.urn)}>
                                            <Tags>
                                                <Tag>
                                                    <CustomAvatar
                                                        size={20}
                                                        photoUrl={photoUrl}
                                                        name={entityRegistry.getDisplayName(
                                                            EntityType.CorpGroup,
                                                            item.entity,
                                                        )}
                                                        style={AVATAR_STYLE}
                                                    />
                                                    {entityRegistry.getDisplayName(EntityType.CorpUser, item.entity)}
                                                </Tag>
                                            </Tags>
                                        </Link>
                                    );
                                })}
                            {!groupSectionExpanded && groupMemberRelationships?.relationships.length > 2 && (
                                <GroupsSeeMoreText onClick={() => setGroupSectionExpanded(!groupSectionExpanded)}>
                                    {`+${groupMemberRelationships?.relationships.length - 2} more`}
                                </GroupsSeeMoreText>
                            )}
                        </TagsSection>
                    </GroupsSection>
                </SideBarSubSection>
                {isProfileOwner && (
                    <EditProfileButton>
                        <Button icon={<EditOutlined />} onClick={() => showEditGroupModal(true)}>
                            Edit Group
                        </Button>
                    </EditProfileButton>
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
