import { Button, Text, Tooltip } from '@components';
import { message } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { Editor } from '@components/components/Editor/Editor';

import analytics, { EventType } from '@app/analytics';

import { useUpdateAssertionMetadataMutation } from '@graphql/assertion.generated';
import { Assertion, Maybe } from '@types';

export const StyledEditor = styled(Editor)`
    &&& {
        .remirror-theme {
            width: 100%;
            overflow: hidden;
        }
        .remirror-editor {
            min-height: 320px;
            max-width: 100%;

            .remirror-editor-content {
                padding: 0;
                max-width: 100%;
            }
        }
        .ProseMirror pre,
        .ProseMirror code {
            white-space: pre-wrap !important;
            word-wrap: break-word;
            overflow-wrap: break-word;
            max-width: 100%;
        }
    }
`;

const Header = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 16px;
`;

type Props = {
    assertion: Maybe<Assertion>;
    editAllowed: boolean;
};

export const AssertionNoteTab = ({ assertion, editAllowed }: Props) => {
    const [updateAssertionMetadata] = useUpdateAssertionMetadataMutation();

    const [note, setNote] = useState(assertion?.info?.note || '');
    const [isEditing, setIsEditing] = useState(!assertion?.info?.note);

    useEffect(() => {
        if (assertion?.info?.note) {
            setNote(assertion.info.note);
        }
    }, [assertion?.info?.note]);

    const handleSave = async () => {
        setIsEditing(false);
        if (!assertion) {
            message.error('Assertion not found');
            return;
        }
        try {
            await updateAssertionMetadata({
                variables: {
                    urn: assertion.urn,
                    input: {
                        description: assertion.info?.description,
                        actions: assertion.actions
                            ? {
                                  onSuccess: assertion.actions.onSuccess.map((action) => ({
                                      type: action.type,
                                  })),
                                  onFailure: assertion.actions.onFailure.map((action) => ({
                                      type: action.type,
                                  })),
                              }
                            : null,
                        note,
                    },
                },
            });
            message.success('Changes saved');
        } catch (error) {
            console.error(error);
            message.error('Encountered an unexpected error while updating assertion note');
            setIsEditing(true);
        }

        analytics.event({
            type: EventType.UpdateAssertionNoteEvent,
            assertionUrn: assertion.urn,
            entityUrn: assertion.monitor?.entity?.urn || '',
            assertionType: assertion.info?.type || '',
        });
    };

    return (
        <div>
            <Header>
                <Text size="md" color="gray">
                    Tips for troubleshooting, and other useful context about this assertion.
                </Text>
                <Tooltip
                    title={!editAllowed ? 'You do not have permission to edit this assertion note' : ''}
                    placement="top"
                >
                    <div>
                        {isEditing ? (
                            <Button onClick={handleSave} disabled={!editAllowed}>
                                Save
                            </Button>
                        ) : (
                            <Button onClick={() => setIsEditing(true)} disabled={!editAllowed}>
                                Edit
                            </Button>
                        )}
                    </div>
                </Tooltip>
            </Header>
            <StyledEditor
                toolbarStyles={{ width: '100%' }}
                content={note}
                readOnly={!isEditing}
                onChange={(content) => setNote(content)}
                placeholder="Add context to help technical owners troubleshoot Data Quality failures."
            />
        </div>
    );
};
