import { message } from 'antd';
import React, { useRef, useState } from 'react';
import styled from 'styled-components/macro';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { validateCustomUrnId } from '@app/shared/textUtil';
import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import { Button, Editor, Input, Modal } from '@src/alchemy-components';
import { Label } from '@src/alchemy-components/components/Input/components';

import { useAddGroupMembersMutation, useCreateGroupMutation } from '@graphql/group.generated';
import { useAddOwnerMutation } from '@graphql/mutations.generated';
import { CorpGroup, EntityType, OwnerEntityType } from '@types';

type Props = {
    onClose: () => void;
    onCreate: (group: CorpGroup) => void;
};

const FormSection = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
    margin-bottom: 16px;
`;

const StyledEditor = styled(Editor)`
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 8px;
`;

const AdvancedContent = styled.div`
    margin-top: 12px;
`;

const AdvancedButton = styled(Button)`
    padding-left: 0;
    padding-right: 0;
    color: ${(props) => props.theme.colors.textSecondary};
`;

export default function CreateGroupModal({ onClose, onCreate }: Props) {
    const { urn: currentUserUrn } = useUserContext();

    const [stagedName, setStagedName] = useState('');
    const [stagedDescription, setStagedDescription] = useState('');
    const [stagedId, setStagedId] = useState<string | undefined>(undefined);
    const [nameError, setNameError] = useState('');
    const [idError, setIdError] = useState('');
    const [showAdvanced, setShowAdvanced] = useState(false);
    const [createGroupMutation] = useCreateGroupMutation();
    const [addOwnerMutation] = useAddOwnerMutation();
    const [addGroupMembersMutation] = useAddGroupMembersMutation();

    const styledEditorRef = useRef<HTMLDivElement>(null);

    const validateName = (value: string) => {
        if (!value || !value.trim()) {
            setNameError('Enter a Group name.');
            return false;
        }
        if (value.length > 50) {
            setNameError('Name must be 50 characters or less.');
            return false;
        }
        setNameError('');
        return true;
    };

    const validateId = (value: string | undefined) => {
        if (value && !validateCustomUrnId(value)) {
            setIdError('Please enter a valid Group ID.');
            return false;
        }
        setIdError('');
        return true;
    };

    const isCreateDisabled = !stagedName.trim() || !!nameError || !!idError;

    const onCreateGroup = () => {
        const isEditorNewlineKeypress =
            document.activeElement !== styledEditorRef.current &&
            !styledEditorRef.current?.contains(document.activeElement);
        if (!isEditorNewlineKeypress) return;
        if (!validateName(stagedName) || !validateId(stagedId)) return;

        createGroupMutation({
            variables: {
                input: {
                    id: stagedId,
                    name: stagedName,
                    description: stagedDescription,
                },
            },
        })
            .then(({ data, errors }) => {
                if (!errors) {
                    analytics.event({
                        type: EventType.CreateGroupEvent,
                    });
                    message.success({
                        content: `Created group!`,
                        duration: 3,
                    });
                    onCreate({
                        urn: data?.createGroup || '',
                        type: EntityType.CorpGroup,
                        name: stagedName,
                        info: {
                            description: stagedDescription,
                        },
                    });
                }
                if (currentUserUrn && data?.createGroup) {
                    addOwnerMutation({
                        variables: {
                            input: {
                                ownerUrn: currentUserUrn,
                                resourceUrn: data.createGroup,
                                ownerEntityType: OwnerEntityType.CorpUser,
                                ownershipTypeUrn: 'urn:li:ownershipType:__system__none',
                            },
                        },
                    }).catch((e) => {
                        console.error(e);
                        message.error({
                            content: `Failed to automatically add you as an owner of the group. Please add yourself as an owner manually.`,
                            duration: 5,
                        });
                    });

                    addGroupMembersMutation({
                        variables: {
                            groupUrn: data.createGroup,
                            userUrns: [currentUserUrn],
                        },
                    }).catch((e) => {
                        console.error(e);
                        message.error({
                            content: `Failed to automatically add you as a member of the group. Please add yourself as a member manually.`,
                            duration: 5,
                        });
                    });
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to create group!: \n ${e.message || ''}`, duration: 3 });
            })
            .finally(() => {
                setStagedName('');
                setStagedDescription('');
            });
        onClose();
    };

    useEnterKeyListener({
        querySelectorToExecuteClick: '#createGroupButton',
    });

    return (
        <Modal
            width={780}
            title="Create new group"
            open
            onCancel={onClose}
            buttons={[
                {
                    text: 'Cancel',
                    variant: 'text',
                    onClick: onClose,
                },
                {
                    text: 'Create',
                    variant: 'filled',
                    disabled: isCreateDisabled,
                    onClick: onCreateGroup,
                    buttonDataTestId: 'modal-create-group-button',
                    id: 'createGroupButton',
                },
            ]}
        >
            <FormSection>
                <Input
                    label="Name"
                    isRequired
                    placeholder="A name for your group"
                    value={stagedName}
                    setValue={(val) => {
                        setStagedName(val);
                        validateName(val);
                    }}
                    error={nameError}
                    maxLength={50}
                />
            </FormSection>
            <FormSection>
                <Label>Description</Label>
                <div ref={styledEditorRef}>
                    <StyledEditor doNotFocus content={stagedDescription} onChange={setStagedDescription} />
                </div>
            </FormSection>
            <AdvancedButton
                variant="text"
                onClick={() => setShowAdvanced(!showAdvanced)}
                icon={{
                    icon: showAdvanced ? 'CaretDown' : 'CaretRight',
                    source: 'phosphor',
                    size: 'md',
                    color: 'gray',
                }}
                iconPosition="right"
            >
                Advanced
            </AdvancedButton>
            {showAdvanced && (
                <AdvancedContent>
                    <FormSection>
                        <Input
                            label="Group Id"
                            placeholder="product_engineering"
                            value={stagedId || ''}
                            setValue={(val) => {
                                setStagedId(val);
                                validateId(val);
                            }}
                            error={idError}
                            helperText="By default, a random UUID will be generated. Provide a custom id to more easily track this group. You cannot change the group id after creation."
                        />
                    </FormSection>
                </AdvancedContent>
            )}
        </Modal>
    );
}
