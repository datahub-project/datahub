import { EditOutlined, MailOutlined, PhoneOutlined, SlackOutlined } from '@ant-design/icons';
import { Button, Divider, Space, Tag, Typography, message } from 'antd';
import React, { useEffect, useState } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import EntityGroups from '@app/entity/shared/EntityGroups';
import {
    AboutSection,
    AboutSectionText,
    EditButton,
    EmptyValue,
    GroupsSection,
    LocationSection,
    LocationSectionText,
    Name,
    SideBar,
    SideBarSubSection,
    SocialDetails,
    Team,
    TitleRole,
    UserDetails,
} from '@app/entity/shared/SidebarStyledComponents';
import UserEditProfileModal from '@app/entity/user/UserEditProfileModal';
import { mapRoleIcon } from '@app/identity/user/UserUtils';
import { useBrowserTitle } from '@app/shared/BrowserTabTitleContext';
import CustomAvatar from '@app/shared/avatar/CustomAvatar';
import { getCountryName } from '@src/app/shared/sidebar/components';

import { useUpdateCorpUserPropertiesMutation } from '@graphql/user.generated';
import { DataHubRole, EntityRelationship } from '@types';

import GlobeIcon from '@images/Globe.svg';

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
    groupsDetails: Array<EntityRelationship>;
    urn: string | undefined;
    dataHubRoles: Array<EntityRelationship>;
    countryCode: string | undefined;
    username: string | undefined;
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
    const {
        name,
        aboutText,
        avatarName,
        email,
        groupsDetails,
        phone,
        photoUrl,
        role,
        slack,
        team,
        dataHubRoles,
        urn,
        countryCode,
        username,
    } = sideBarData;

    const [updateCorpUserPropertiesMutation] = useUpdateCorpUserPropertiesMutation();

    const [groupSectionExpanded, setGroupSectionExpanded] = useState(false);
    const [editProfileModal, showEditProfileModal] = useState(false);
    /* eslint-disable @typescript-eslint/no-unused-vars */
    const me = useUserContext();
    const isProfileOwner = me?.user?.urn === urn;

    const { updateTitle } = useBrowserTitle();

    useEffect(() => {
        // You can use the title and updateTitle function here
        // For example, updating the title when the component mounts
        if (name) {
            updateTitle(`User | ${name}`);
        }
        // // Don't forget to clean up the title when the component unmounts
        return () => {
            if (name) {
                // added to condition for rerendering issue
                updateTitle('');
            }
        };
    }, [name, updateTitle]);

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
            .then(() => {
                message.success({
                    content: `Changes saved.`,
                    duration: 3,
                });
                refetch();
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to Save changes!: \n ${e.message || ''}`, duration: 3 });
            });
    };
    const dataHubRoleName = dataHubRoles && dataHubRoles.length > 0 && (dataHubRoles[0]?.entity as DataHubRole).name;

    const maybeCountryName = getCountryName(countryCode ?? '');
    return (
        <>
            <SideBar>
                <SideBarSubSection className={isProfileOwner ? '' : 'fullView'}>
                    <CustomAvatar size={160} photoUrl={photoUrl} name={avatarName} style={AVATAR_STYLE} />
                    <Name>{name || <EmptyValue />}</Name>
                    <UserDetails>{username || <EmptyValue />}</UserDetails>
                    {role && <TitleRole>{role}</TitleRole>}
                    {team && <Team>{team}</Team>}
                    {dataHubRoleName && <Tag icon={mapRoleIcon(dataHubRoleName)}>{dataHubRoleName}</Tag>}
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
                    {maybeCountryName != null ? (
                        <LocationSection>
                            Location
                            <br />
                            <LocationSectionText>
                                <img src={GlobeIcon} alt="Manage Users" style={{ display: 'inline' }} /> &nbsp;
                                {maybeCountryName}
                            </LocationSectionText>
                            <Divider className="divider-aboutSection" />
                        </LocationSection>
                    ) : null}
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
                open={editProfileModal}
                onClose={() => showEditProfileModal(false)}
                onSave={() => {
                    refetch();
                }}
                editModalData={getEditModalData}
            />
        </>
    );
}
