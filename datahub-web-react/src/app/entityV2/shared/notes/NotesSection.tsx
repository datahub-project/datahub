import SchemaEditableContext from '@app/shared/SchemaEditableContext';
import AddRoundedIcon from '@mui/icons-material/AddRounded';
import DeleteOutlineOutlinedIcon from '@mui/icons-material/DeleteOutlineOutlined';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import { Modal } from 'antd';
import moment from 'moment';
import React, { useState } from 'react';
import styled from 'styled-components';
import { useDeletePostMutation } from '../../../../graphql/post.generated';
import { Post } from '../../../../types.generated';
import CustomAvatar from '../../../shared/avatar/CustomAvatar';
import { COLORS } from '../../../sharedV2/colors';
import CreateEntityAnnouncementModal from '../announce/CreateEntityAnnouncementModal';
import CompactMarkdownViewer from '../tabs/Documentation/components/CompactMarkdownViewer';
import EmptySectionText from '../containers/profile/sidebar/EmptySectionText';
import SectionActionButton from '../containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '../containers/profile/sidebar/SidebarSection';

const ContentWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

interface Props {
    urn: string;
    subResource?: string;
    notes?: Post[];
    refetch?: () => void;
    showEmpty?: boolean;
}

export default function NotesSection({ urn, subResource, notes, refetch, showEmpty }: Props) {
    const isSchemaEditable = React.useContext(SchemaEditableContext);
    const [showAddModal, setShowAddModal] = useState(false);

    const content = notes?.length ? (
        <ContentWrapper>
            {notes?.map((note) => (
                <SidebarNote
                    key={note.urn}
                    note={note}
                    parentUrn={urn}
                    parentSubResource={subResource}
                    refetch={refetch}
                />
            ))}
        </ContentWrapper>
    ) : (
        <EmptySectionText message="No notes yet" />
    );

    if (!showEmpty && !notes?.length) return null;
    return (
        <>
            <SidebarSection
                title="Notes"
                content={content}
                extra={
                    isSchemaEditable && (
                        <SectionActionButton
                            button={<AddRoundedIcon />}
                            onClick={(event) => {
                                setShowAddModal(true);
                                event.stopPropagation();
                            }}
                        />
                    )
                }
            />
            {showAddModal && (
                <CreateEntityAnnouncementModal
                    urn={urn}
                    subResource={subResource}
                    // refetch after 2 seconds on create to ensure the new note is visible
                    onCreate={refetch}
                    onClose={() => setShowAddModal(false)}
                />
            )}
        </>
    );
}

const NoteEditWrapper = styled.div`
    display: none;
    position: relative;
`;

const NoteEditIcons = styled.div`
    display: flex;
    gap: 5px;

    position: absolute;
    right: -9px;
`;

const NoteWrapper = styled.div`
    display: flex;
    justify-content: space-between;
    max-width: 100%;

    :hover {
        ${NoteEditWrapper} {
            display: inline;
        }
    }
`;

const NoteContent = styled.div`
    border-left: 2px solid ${COLORS.blue_5};
    padding: 2px 0 5px 10px;
    display: flex;
    flex-direction: column;
    gap: 2px;
    max-width: inherit;
`;

const NoteHeader = styled.div`
    align-items: center;
    display: flex;
    font-size: 12px;
    color: ${COLORS.blue_10};
`;

const NoteTime = styled.div`
    overflow: hidden;
    text-overflow: ellipsis;
`;

const NoteOwner = styled.div`
    min-width: 24px;
`;

const NoteTitle = styled.div`
    font-size: 16px;
    font-weight: 600;
    line-height: 20px;
    max-width: inherit;
    overflow: hidden;
    text-overflow: ellipsis;
`;

const NoteDescriptionContainer = styled.div`
    flex: 1;

    .remirror-editor.ProseMirror {
        font-size: 12px;
        padding: 0;
        max-width: 400px;
    }

    p {
        margin-bottom: 0;
    }
`;

interface NoteProps {
    note: Post;
    parentUrn: string;
    parentSubResource?: string;
    refetch?: () => void;
}

function SidebarNote({ note, parentUrn, parentSubResource, refetch }: NoteProps) {
    const isSchemaEditable = React.useContext(SchemaEditableContext);
    const [showEditModal, setShowEditModal] = useState(false);
    const [deletePost] = useDeletePostMutation();

    const time = moment(note.lastModified.time);
    const isToday = time.isSame(moment(), 'day');
    const isYesterday = time.isSame(moment().subtract(1, 'day'), 'day');

    // parsedName should strip the urn:li:corpuser: prefix from actor
    const parsedName = note?.lastModified?.actor?.split(':')?.slice(-1)[0] || '';

    return (
        <NoteWrapper>
            <NoteContent>
                <NoteHeader>
                    <NoteOwner>{note.lastModified.actor && <CustomAvatar size={18} name={parsedName} />}</NoteOwner>
                    <NoteTime>
                        {isToday && 'Today'}
                        {isYesterday && 'Yesterday'}
                        {!isToday && !isYesterday && time.format('MMM D, YYYY')}
                        {' at '}
                        {time.format('h:mm A')}
                    </NoteTime>
                </NoteHeader>
                <NoteTitle>{note.content.title}</NoteTitle>
                {note.content.description && (
                    <NoteDescriptionContainer>
                        <CompactMarkdownViewer content={note.content.description} />
                    </NoteDescriptionContainer>
                )}
            </NoteContent>
            {isSchemaEditable && (
                <NoteEditWrapper>
                    <NoteEditIcons>
                        <SectionActionButton button={<EditOutlinedIcon />} onClick={() => setShowEditModal(true)} />
                        <SectionActionButton
                            button={<DeleteOutlineOutlinedIcon />}
                            onClick={() =>
                                onDeleteNote(() => deletePost({ variables: { urn: note.urn } }).then(refetch))
                            }
                        />
                    </NoteEditIcons>
                </NoteEditWrapper>
            )}
            {showEditModal && (
                <CreateEntityAnnouncementModal
                    urn={parentUrn}
                    subResource={parentSubResource}
                    editData={{ urn: note.urn, ...note.content }}
                    onClose={() => setShowEditModal(false)}
                    onEdit={refetch}
                />
            )}
        </NoteWrapper>
    );
}

function onDeleteNote(onDelete: () => void) {
    Modal.confirm({
        title: 'Delete Note',
        content: `Are you sure you want to remove this note?`,
        onOk: onDelete,
        onCancel() {},
        okText: 'Yes',
        maskClosable: true,
        closable: true,
    });
}
