import { Divider, message, Space, Button, Tag, Typography } from 'antd';
import React, { useState } from 'react';
import { EditOutlined, MailOutlined, PhoneOutlined, SlackOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { useUpdateCorpUserPropertiesMutation } from '../../../graphql/user.generated';
import { EntityType } from '../../../types.generated';

import UserEditProfileModal from './UserEditProfileModal';
import { ExtendedEntityRelationshipsResult } from './type';
import CustomAvatar from '../../shared/avatar/CustomAvatar';
import { useEntityRegistry } from '../../useEntityRegistry';
import { useGetAuthenticatedUser } from '../../useGetAuthenticatedUser';
import {
    SideBar,
    SideBarSubSection,
    EmptyValue,
    SocialDetails,
    EditButton,
    AboutSection,
    AboutSectionText,
    GroupsSection,
    TagsSection,
    Tags,
    GroupsSeeMoreText,
    Name,
    Role,
    Team,
} from '../shared/SidebarStyledComponents';

const { Paragraph } = Typography;

type SideBarData = {
    photoUrl: string | undefined;
    avatarName: string | undefined;
    name: string | undefined;
    role: string | undefined;
    team: string | undefined;
    email: string | undefined;
    slack: string | undefined;
    phone: string | undefined;
    aboutText: string | undefined;
    groupsDetails: ExtendedEntityRelationshipsResult;
    urn: string | undefined;
};

type Props = {
    sideBarData: SideBarData;
    refetch: () => void;
};

const AVATAR_STYLE = { marginTop: '14px' };

/**
 * Responsible for reading & writing users.
 */
export default function UserInfoSideBar({ sideBarData, refetch }: Props) {
    const { name, aboutText, avatarName, email, groupsDetails, phone, photoUrl, role, slack, team, urn } = sideBarData;

    const [updateCorpUserPropertiesMutation] = useUpdateCorpUserPropertiesMutation();
    const entityRegistry = useEntityRegistry();

    const [groupSectionExpanded, setGroupSectionExpanded] = useState(false);
    const [editProfileModal, showEditProfileModal] = useState(false);
    /* eslint-disable @typescript-eslint/no-unused-vars */
    const me = useGetAuthenticatedUser();
    const isProfileOwner = me?.corpUser?.urn === urn;

    const getEditModalData = {
        urn,
        name,
        title: role,
        team,
        email,
        image: photoUrl,
        slack,
        phone,
    };

    // About Text save
    const onSaveAboutMe = (inputString) => {
        updateCorpUserPropertiesMutation({
            variables: {
                urn: urn || '',
                input: {
                    aboutMe: inputString,
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
                    <CustomAvatar size={160} photoUrl={photoUrl} name={avatarName} style={AVATAR_STYLE} />
                    <Name>{name || <EmptyValue />}</Name>
                    {role && <Role>{role}</Role>}
                    {team && <Team>{team}</Team>}
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
                    <SocialDetails>
                        <Space>
                            <PhoneOutlined />
                            {phone || <EmptyValue />}
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
                        Groups
                        <TagsSection>
                            {groupsDetails?.relationships.length === 0 && <EmptyValue />}
                            {!groupSectionExpanded &&
                                groupsDetails?.relationships.slice(0, 2).map((item) => {
                                    return (
                                        <Link to={entityRegistry.getEntityUrl(EntityType.CorpGroup, item.entity.urn)}>
                                            <Tags>
                                                <Tag>
                                                    {entityRegistry.getDisplayName(EntityType.CorpGroup, item.entity)}
                                                </Tag>
                                            </Tags>
                                        </Link>
                                    );
                                })}
                            {groupSectionExpanded &&
                                groupsDetails?.relationships.length > 2 &&
                                groupsDetails?.relationships.map((item) => {
                                    return (
                                        <Link to={entityRegistry.getEntityUrl(EntityType.CorpGroup, item.entity.urn)}>
                                            <Tags>
                                                <Tag>
                                                    {entityRegistry.getDisplayName(EntityType.CorpGroup, item.entity)}
                                                </Tag>
                                            </Tags>
                                        </Link>
                                    );
                                })}
                            {!groupSectionExpanded && groupsDetails?.relationships.length > 2 && (
                                <GroupsSeeMoreText onClick={() => setGroupSectionExpanded(!groupSectionExpanded)}>
                                    {`+${groupsDetails?.relationships.length - 2} more`}
                                </GroupsSeeMoreText>
                            )}
                        </TagsSection>
                    </GroupsSection>
                </SideBarSubSection>
                {isProfileOwner && (
                    <EditButton>
                        <Button icon={<EditOutlined />} onClick={() => showEditProfileModal(true)}>
                            Edit Profile
                        </Button>
                    </EditButton>
                )}
            </SideBar>
            {/* Modal */}
            <UserEditProfileModal
                visible={editProfileModal}
                onClose={() => showEditProfileModal(false)}
                onSave={() => {
                    refetch();
                }}
                editModalData={getEditModalData}
            />
        </>
    );
}
