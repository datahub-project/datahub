import { Divider, message, Space, Button, Tag, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { EditOutlined, MailOutlined, PhoneOutlined, SlackOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { useUpdateCorpUserPropertiesMutation } from '../../../graphql/user.generated';
import { EntityType } from '../../../types.generated';

import UserEditProfileModal from './UserEditProfileModal';
import { ExtendedEntityRelationshipsResult } from './type';
import CustomAvatar from '../../shared/avatar/CustomAvatar';
import { useEntityRegistry } from '../../useEntityRegistry';
import { useGetAuthenticatedUser } from '../../useGetAuthenticatedUser';

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
 * Styled Components
 */
export const SideBar = styled.div`
    padding: 0 0 0 17px;
    text-align: center;

    font-style: normal;
    font-weight: bold;
    height: calc(100vh - 60px);
    position: relative;

    &&& .ant-avatar.ant-avatar-icon {
        font-size: 46px !important;
    }

    .divider-infoSection {
        margin: 18px 0px 18px 0;
    }
    .divider-aboutSection {
        margin: 23px 0px 11px 0;
    }
    .divider-groupsSection {
        margin: 23px 0px 11px 0;
    }
`;

export const SideBarSubSection = styled.div`
    height: calc(100vh - 135px);
    overflow: auto;
    padding-right: 18px;
    &.fullView {
        height: calc(100vh - 70px);
    }
    &::-webkit-scrollbar {
        height: 12px;
        width: 1px;
        background: #d6d6d6;
    }
    &::-webkit-scrollbar-thumb {
        background: #d6d6d6;
        -webkit-border-radius: 1ex;
        -webkit-box-shadow: 0px 1px 2px rgba(0, 0, 0, 0.75);
    }
`;

export const EmptyValue = styled.div`
    &:after {
        content: 'None';
        color: #b7b7b7;
        font-style: italic;
        font-weight: 100;
    }
`;

export const Name = styled.div`
    font-size: 20px;
    line-height: 28px;
    color: #262626;
    margin: 13px 0 7px 0;
`;

export const Role = styled.div`
    font-size: 14px;
    line-height: 22px;
    color: #595959;
    margin-bottom: 7px;
`;

export const Team = styled.div`
    font-size: 12px;
    line-height: 20px;
    color: #8c8c8c;
`;

export const SocialDetails = styled.div`
    font-size: 12px;
    line-height: 20px;
    color: #262626;
    text-align: left;
    margin: 6px 0;
`;

export const EditProfileButton = styled.div`
    bottom: 24px;
    position: absolute;
    right: 27px;
    width: 80%;
    left: 50%;
    -webkit-transform: translateX(-50%);
    -moz-transform: translateX(-50%);
    transform: translateX(-50%);

    button {
        width: 100%;
        font-size: 12px;
        line-height: 20px;
        color: #262626;
    }
`;

export const AboutSection = styled.div`
    text-align: left;
    font-weight: bold;
    font-size: 14px;
    line-height: 22px;
    color: #262626;
`;

export const AboutSectionText = styled.div`
    font-size: 12px;
    font-weight: 100;
    line-height: 15px;
    padding: 5px 0;

    &&& .ant-typography {
        margin-bottom: 0;
    }
    &&& .ant-typography-edit-content {
        padding-left: 15px;
        padding-top: 5px;
    }
`;

export const GroupsSection = styled.div`
    text-align: left;
    font-weight: bold;
    font-size: 14px;
    line-height: 22px;
    color: #262626;
`;

export const TagsSection = styled.div`
    height: calc(75vh - 460px);
    padding: 5px;
`;

export const NoDataFound = styled.span`
    font-size: 12px;
    color: #262626;
    font-weight: 100;
`;

export const Tags = styled.div`
    margin-top: 5px;
`;

export const GroupsSeeMoreText = styled.span`
    font-weight: 500;
    font-size: 12px;
    line-height: 20px;
    color: #1890ff;
    cursor: pointer;
`;

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
                    <Role>{role || <EmptyValue />}</Role>
                    <Team>{team || <EmptyValue />}</Team>
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
                                                    {item.entity.info.displayName || item.entity.name || <EmptyValue />}
                                                </Tag>
                                            </Tags>
                                        </Link>
                                    );
                                })}
                            {groupSectionExpanded &&
                                groupsDetails?.relationships.length > 2 &&
                                groupsDetails?.relationships.map((item) => {
                                    return (
                                        <Tags>
                                            <Tag>
                                                {item.entity.info.displayName || item.entity.name || <EmptyValue />}
                                            </Tag>
                                        </Tags>
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
                    <EditProfileButton>
                        <Button icon={<EditOutlined />} onClick={() => showEditProfileModal(true)}>
                            Edit Profile
                        </Button>
                    </EditProfileButton>
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
