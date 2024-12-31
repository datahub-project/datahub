import { Divider, message, Space, Button, Typography, Row, Col, Tooltip } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { EditOutlined, LockOutlined, MailOutlined, SlackOutlined } from '@ant-design/icons';
import { useHistory, useRouteMatch } from 'react-router-dom';
import { useUpdateCorpGroupPropertiesMutation } from '../../../graphql/group.generated';
import { EntityRelationshipsResult, Ownership } from '../../../types.generated';
import { useUpdateNameMutation } from '../../../graphql/mutations.generated';

import GroupEditModal from './GroupEditModal';
import CustomAvatar from '../../shared/avatar/CustomAvatar';
import GroupOwnerSideBarSection from './GroupOwnerSideBarSection';
import {
    SideBar,
    SideBarSubSection,
    EmptyValue,
    SocialDetails,
    EditButton,
    GroupsSection,
    AboutSection,
} from '../shared/SidebarStyledComponents';
import GroupMembersSideBarSection from './GroupMembersSideBarSection';
import { useUserContext } from '../../context/useUserContext';
import { useBrowserTitle } from '../../shared/BrowserTabTitleContext';
import StripMarkdownText, { removeMarkdown } from '../shared/components/styled/StripMarkdownText';
import { Editor } from '../shared/tabs/Documentation/components/editor/Editor';
import EditGroupDescriptionModal from './EditGroupDescriptionModal';
import { REDESIGN_COLORS } from '../shared/constants';

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
    urn: string;
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

const GroupTitle = styled(Typography.Title)`
    max-width: 260px;
    word-wrap: break-word;
    width: 140px;

    &&& {
        margin-bottom: 0;
        word-break: break-all;
        margin-left: 10px;
    }

    .ant-typography-edit {
        font-size: 16px;
        margin-left: 10px;
    }
`;

const EditIcon = styled(EditOutlined)`
    cursor: pointer;
    color: ${REDESIGN_COLORS.BLUE};
`;
const AddNewDescription = styled(Button)`
    display: none;
    margin: -4px;
    width: 140px;
`;

const StyledViewer = styled(Editor)`
    padding-right: 8px;
    display: block;

    .remirror-editor.ProseMirror {
        padding: 0;
    }
`;

const DescriptionContainer = styled.div`
    position: relative;
    display: flex;
    flex-direction: column;
    width: 100%;
    text-align:left;
    font-weight: normal;
    font
    min-height: 22px;

    &:hover ${AddNewDescription} {
        display: block;
    }
    & ins.diff {
        background-color: #b7eb8f99;
        text-decoration: none;
        &:hover {
            background-color: #b7eb8faa;
        }
    }
    & del.diff {
        background-color: #ffa39e99;
        text-decoration: line-through;
        &: hover {
            background-color: #ffa39eaa;
        }
    }
`;

const ExpandedActions = styled.div`
    height: 10px;
`;
const ReadLessText = styled(Typography.Link)`
    margin-right: 4px;
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

    const { updateTitle } = useBrowserTitle();

    useEffect(() => {
        // You can use the title and updateTitle function here
        // For example, updating the title when the component mounts
        if (name) {
            updateTitle(`Group | ${name}`);
        }
        // // Don't forget to clean up the title when the component unmounts
        return () => {
            if (name) {
                // added to condition for rerendering issue
                updateTitle('');
            }
        };
    }, [name, updateTitle]);

    /* eslint-disable @typescript-eslint/no-unused-vars */
    const [editGroupModal, showEditGroupModal] = useState(false);
    const me = useUserContext();
    const canEditGroup = me?.platformPrivileges?.manageIdentities;
    const [groupTitle, setGroupTitle] = useState(name);
    const [expanded, setExpanded] = useState(false);
    const [isUpdatingDescription, SetIsUpdatingDescription] = useState(false);
    const [stagedDescription, setStagedDescription] = useState(aboutText);

    const [updateName] = useUpdateNameMutation();
    const overLimit = removeMarkdown(aboutText || '').length > 80;
    const ABBREVIATED_LIMIT = 80;

    useEffect(() => {
        setStagedDescription(aboutText);
    }, [aboutText]);

    useEffect(() => {
        setGroupTitle(groupTitle);
    }, [groupTitle]);

    // Update Group Title
    // eslint-disable-next-line @typescript-eslint/no-shadow
    const handleTitleUpdate = async (name: string) => {
        setGroupTitle(name);
        await updateName({ variables: { input: { name, urn } } })
            .then(() => {
                message.success({ content: 'Name Updated', duration: 2 });
                refetch();
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Failed to update name: \n ${e.message || ''}`, duration: 3 });
                }
            });
    };

    const getEditModalData = {
        urn,
        email,
        slack,
        photoUrl,
    };

    // About Text save
    const onSaveAboutMe = () => {
        updateCorpGroupPropertiesMutation({
            variables: {
                urn: urn || '',
                input: {
                    description: stagedDescription,
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
                            <GroupTitle level={3} editable={canEditGroup ? { onChange: handleTitleUpdate } : false}>
                                {groupTitle}
                            </GroupTitle>
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
                        <Row>
                            <Col span={22}>{TITLES.about}</Col>
                            <Col span={2}>
                                <EditIcon onClick={() => SetIsUpdatingDescription(true)} data-testid="edit-icon" />
                            </Col>
                        </Row>
                    </AboutSection>
                    <DescriptionContainer>
                        {(aboutText && expanded) || !overLimit ? (
                            <>
                                {/* Read only viewer for displaying group description */}
                                <StyledViewer content={aboutText} readOnly />
                                <ExpandedActions>
                                    {overLimit && (
                                        <ReadLessText
                                            onClick={() => {
                                                setExpanded(false);
                                            }}
                                        >
                                            Read Less
                                        </ReadLessText>
                                    )}
                                </ExpandedActions>
                            </>
                        ) : (
                            <>
                                {/* Display abbreviated description with option to read more */}
                                <StripMarkdownText
                                    limit={ABBREVIATED_LIMIT}
                                    readMore={
                                        <>
                                            <Typography.Link
                                                onClick={() => {
                                                    setExpanded(true);
                                                }}
                                            >
                                                Read More
                                            </Typography.Link>
                                        </>
                                    }
                                    shouldWrap
                                >
                                    {aboutText}
                                </StripMarkdownText>
                            </>
                        )}
                    </DescriptionContainer>
                    {/* Modal for updating group description */}
                    {isUpdatingDescription && (
                        <EditGroupDescriptionModal
                            onClose={() => {
                                SetIsUpdatingDescription(false);
                                setStagedDescription(aboutText);
                            }}
                            onSaveAboutMe={onSaveAboutMe}
                            setStagedDescription={setStagedDescription}
                            stagedDescription={stagedDescription}
                        />
                    )}
                    <Divider className="divider-groupsSection" />
                    <GroupsSection>
                        <GroupOwnerSideBarSection ownership={ownership} urn={urn || ''} refetch={refetch} />
                    </GroupsSection>
                    <Divider className="divider-groupsSection" />
                    <GroupsSection>
                        <GroupMembersSideBarSection
                            total={groupMemberRelationships?.total || 0}
                            relationships={groupMemberRelationships?.relationships || []}
                            onSeeMore={() => history.replace(`${url}/members`)}
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
                open={editGroupModal}
                onClose={() => showEditGroupModal(false)}
                onSave={() => {
                    refetch();
                }}
                editModalData={getEditModalData}
            />
        </>
    );
}
