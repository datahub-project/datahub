import { Divider, message, Space, Button, Typography } from 'antd';
import React, { useState } from 'react';
import { EditOutlined, MailOutlined, PhoneOutlined, SlackOutlined } from '@ant-design/icons';
import { useUpdateCorpUserPropertiesMutation } from '../../../graphql/user.generated';
import { EntityRelationshipsResult } from '../../../types.generated';
import UserEditProfileModal from './UserEditProfileModal';
import CustomAvatar from '../../shared/avatar/CustomAvatar';
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
    Name,
    Role,
    Team,
} from '../shared/SidebarStyledComponents';
import EntityGroups from '../shared/EntityGroups';

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
    groupsDetails: EntityRelationshipsResult;
    urn: string | undefined;
};

type Props = {
    sideBarData: SideBarData;
    refetch: () => void;
};

const AVATAR_STYLE = { marginTop: '14px' };

/**
 * UserInfoSideBar- Sidebar section for users profiles.
 */
export default function UserInfoSideBar({ sideBarData, refetch }: Props) {
    const { name, aboutText, avatarName, email, groupsDetails, phone, photoUrl, role, slack, team, urn } = sideBarData;

    const [updateCorpUserPropertiesMutation] = useUpdateCorpUserPropertiesMutation();

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
                        <EntityGroups
                            readMore={groupSectionExpanded}
                            setReadMore={() => setGroupSectionExpanded(!groupSectionExpanded)}
                            groupMemberRelationships={groupsDetails}
                        />
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
