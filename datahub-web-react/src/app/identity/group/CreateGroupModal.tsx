import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import { message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { ActorsSearchSelect } from '@app/entityV2/shared/EntitySearchSelect/ActorsSearchSelect';
import { ActorEntity } from '@app/entityV2/shared/utils/actorUtils';
import { AdvancedButton, AdvancedContent, FormSection } from '@app/identity/group/CreateGroupModal.components';
import { validateCustomUrnId } from '@app/shared/textUtil';
import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import { Input, Modal, TextArea } from '@src/alchemy-components';

import { useAddGroupMembersMutation, useCreateGroupMutation } from '@graphql/group.generated';
import { useAddOwnerMutation } from '@graphql/mutations.generated';
import { CorpGroup, EntityType, OwnerEntityType } from '@types';

type Props = {
    onClose: () => void;
    onCreate: (group: CorpGroup) => void;
};

export default function CreateGroupModal({ onClose, onCreate }: Props) {
    const { t } = useTranslation('entity.identity');
    const { t: tc } = useTranslation('common.actions');
    const { urn: currentUserUrn } = useUserContext();

    const [stagedName, setStagedName] = useState('');
    const [stagedDescription, setStagedDescription] = useState('');
    const [stagedId, setStagedId] = useState<string | undefined>(undefined);
    const [stagedMemberUrns, setStagedMemberUrns] = useState<string[]>([]);
    const [nameError, setNameError] = useState('');
    const [idError, setIdError] = useState('');
    const [showAdvanced, setShowAdvanced] = useState(false);
    const [createGroupMutation] = useCreateGroupMutation();
    const [addOwnerMutation] = useAddOwnerMutation();
    const [addGroupMembersMutation] = useAddGroupMembersMutation();

    const validateName = (value: string) => {
        if (!value || !value.trim()) {
            setNameError(t('groups.createModal.name.validationError.empty'));
            return false;
        }
        if (value.length > 50) {
            setNameError(t('groups.createModal.name.validationError.tooLong'));
            return false;
        }
        setNameError('');
        return true;
    };

    const validateId = (value: string | undefined) => {
        if (value && !validateCustomUrnId(value)) {
            setIdError(t('groups.createModal.groupId.validationError'));
            return false;
        }
        setIdError('');
        return true;
    };

    const isCreateDisabled = !stagedName.trim() || !!nameError || !!idError;

    const onCreateGroup = () => {
        if (document.activeElement instanceof HTMLTextAreaElement) return;
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
                        content: t('groups.createSuccess'),
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
                            content: t('groups.createOwnerError'),
                            duration: 5,
                        });
                    });

                    const allMemberUrns = [currentUserUrn, ...stagedMemberUrns.filter((u) => u !== currentUserUrn)];
                    addGroupMembersMutation({
                        variables: {
                            groupUrn: data.createGroup,
                            userUrns: allMemberUrns,
                        },
                    }).catch((e) => {
                        console.error(e);
                        message.error({
                            content: t('groups.addMembersError'),
                            duration: 5,
                        });
                    });
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: t('groups.createError', { error: e.message || '' }), duration: 3 });
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
            title={t('groups.createModal.title')}
            open
            onCancel={onClose}
            dataTestId="create-group-modal"
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onClose,
                },
                {
                    text: tc('create'),
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
                    id="name"
                    inputTestId="group-name-input"
                    label={t('groups.createModal.name.label')}
                    isRequired
                    placeholder={t('groups.createModal.name.placeholder')}
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
                <TextArea
                    id="description"
                    data-testid="group-description-input"
                    label={t('groups.createModal.description.label')}
                    placeholder={t('groups.createModal.description.placeholder')}
                    value={stagedDescription}
                    onChange={(e) => setStagedDescription(e.target.value)}
                    rows={3}
                />
            </FormSection>
            <FormSection>
                <ActorsSearchSelect
                    label={t('groups.createModal.members.label')}
                    selectedActorUrns={stagedMemberUrns}
                    onUpdate={(actors: ActorEntity[]) => setStagedMemberUrns(actors.map((a) => a.urn))}
                    placeholder={t('groups.createModal.members.placeholder')}
                    entityTypes={[EntityType.CorpUser]}
                />
            </FormSection>
            <AdvancedButton
                variant="text"
                onClick={() => setShowAdvanced(!showAdvanced)}
                icon={{
                    icon: showAdvanced ? CaretDown : CaretRight,
                    size: 'md',
                    color: 'gray',
                }}
                iconPosition="right"
            >
                {t('groups.createModal.advanced')}
            </AdvancedButton>
            {showAdvanced && (
                <AdvancedContent>
                    <FormSection>
                        <Input
                            id="groupId"
                            inputTestId="group-id-input"
                            label={t('groups.createModal.groupId.label')}
                            placeholder={t('groups.createModal.groupId.placeholder')}
                            value={stagedId || ''}
                            setValue={(val) => {
                                setStagedId(val);
                                validateId(val);
                            }}
                            error={idError}
                            helperText={t('groups.createModal.groupId.helperText')}
                        />
                    </FormSection>
                </AdvancedContent>
            )}
        </Modal>
    );
}
