import { Button, Form, Modal, Select, Typography, message } from 'antd';
import React, { useCallback, useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { handleBatchError } from '@app/entity/shared/utils';
import { ActorsSearchSelect } from '@app/entityV2/shared/EntitySearchSelect/ActorsSearchSelect';
import { ActorEntity } from '@app/entityV2/shared/utils/actorUtils';
import { useOwnershipTypes } from '@app/sharedV2/owners/useOwnershipTypes';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { getModalDomContainer } from '@utils/focus';

import { useBatchAddOwnersMutation, useBatchRemoveOwnersMutation } from '@graphql/mutations.generated';
import { Entity, EntityType, OwnerEntityType, OwnershipTypeEntity } from '@types';

const FormSection = styled.div`
    margin-bottom: 16px;
`;

export enum OperationType {
    ADD,
    REMOVE,
}

type Props = {
    urns: string[];
    defaultOwnerType?: string;
    hideOwnerType?: boolean | undefined;
    operationType?: OperationType;
    onCloseModal: () => void;
    refetch?: () => Promise<any>;
    entityType?: EntityType;
    onOkOverride?: (result: SelectedOwner[]) => void;
    title?: string;
    defaultValues?: { urn: string; entity?: Entity | null }[];
};

type SelectedOwner = {
    label: string | React.ReactNode;
    value: {
        ownerUrn: string;
        ownerEntityType: EntityType;
    };
};

export const EditOwnersModal = ({
    urns,
    hideOwnerType,
    defaultOwnerType,
    operationType = OperationType.ADD,
    onCloseModal,
    refetch,
    entityType,
    onOkOverride,
    title,
    defaultValues,
}: Props) => {
    const entityRegistry = useEntityRegistry();
    const { t } = useTranslation('entity.shared.containers');
    const { t: tc } = useTranslation('common.actions');
    const { t: tcl } = useTranslation('common.labels');

    const [batchAddOwnersMutation] = useBatchAddOwnersMutation();
    const [batchRemoveOwnersMutation] = useBatchRemoveOwnersMutation();
    const { ownershipTypes, loading: ownershipTypesLoading } = useOwnershipTypes();

    const defaultUrns = (defaultValues || []).map((v) => v.urn);
    const [selectedActorUrns, setSelectedActorUrns] = useState<string[]>(defaultUrns);
    const [selectedActors, setSelectedActors] = useState<ActorEntity[]>([]);
    const [selectedOwnerType, setSelectedOwnerType] = useState<string | undefined>(undefined);

    useEffect(() => {
        if (ownershipTypes.length) {
            const defaultType = ownershipTypes.find((type) => type.urn === defaultOwnerType);
            setSelectedOwnerType(defaultType?.urn || ownershipTypes[0].urn);
        }
    }, [ownershipTypes, defaultOwnerType]);

    const handleActorsUpdate = useCallback((actors: ActorEntity[]) => {
        setSelectedActors(actors);
        setSelectedActorUrns(actors.map((a) => a.urn));
    }, []);

    const onModalClose = () => {
        setSelectedActorUrns([]);
        setSelectedActors([]);
        setSelectedOwnerType(defaultOwnerType || undefined);
        onCloseModal();
    };

    const emitAnalytics = () => {
        if (urns.length > 1) {
            analytics.event({
                type: EventType.BatchEntityActionEvent,
                actionType: EntityActionType.UpdateOwnership,
                entityUrns: urns,
            });
        } else {
            analytics.event({
                type: EventType.EntityActionEvent,
                actionType: EntityActionType.UpdateOwnership,
                entityType,
                entityUrn: urns[0],
            });
        }
    };

    const batchAddOwners = async (inputs) => {
        try {
            await batchAddOwnersMutation({
                variables: {
                    input: {
                        owners: inputs,
                        resources: urns.map((urn) => ({ resourceUrn: urn })),
                    },
                },
            });
            message.success({ content: t('sidebar.ownership.ownersAddedSuccess'), duration: 2 });
            emitAnalytics();
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error(
                    handleBatchError(urns, e, {
                        content: t('sidebar.ownership.ownersAddFailed', { message: e.message || '' }),
                        duration: 3,
                    }),
                );
            }
        } finally {
            refetch?.();
            onModalClose();
        }
    };

    const batchRemoveOwners = async (inputs) => {
        try {
            await batchRemoveOwnersMutation({
                variables: {
                    input: {
                        ownerUrns: inputs.map((input) => input.ownerUrn),
                        resources: urns.map((urn) => ({ resourceUrn: urn })),
                    },
                },
            });
            message.success({ content: t('sidebar.ownership.ownersRemovedSuccess'), duration: 2 });
            emitAnalytics();
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error(
                    handleBatchError(urns, e, {
                        content: t('sidebar.ownership.ownersRemoveFailed', { message: e.message || '' }),
                        duration: 3,
                    }),
                );
            }
        } finally {
            refetch?.();
            onModalClose();
        }
    };

    const onOk = () => {
        if (selectedActorUrns.length === 0) {
            return;
        }

        if (onOkOverride) {
            const selectedOwners: SelectedOwner[] = selectedActors.map((actor) => ({
                label: entityRegistry.getDisplayName(actor.type, actor),
                value: {
                    ownerUrn: actor.urn,
                    ownerEntityType: actor.type,
                },
            }));
            onOkOverride(selectedOwners);
            return;
        }

        const inputs = selectedActorUrns.map((urn) => {
            const actor = selectedActors.find((a) => a.urn === urn);
            const ownerEntityType =
                actor?.type === EntityType.CorpGroup ? OwnerEntityType.CorpGroup : OwnerEntityType.CorpUser;
            return {
                ownerUrn: urn,
                ownerEntityType,
                ownershipTypeUrn: selectedOwnerType,
            };
        });

        if (operationType === OperationType.ADD) {
            batchAddOwners(inputs);
        } else {
            batchRemoveOwners(inputs);
        }
    };

    return (
        <Modal
            title={
                title ||
                (operationType === OperationType.ADD
                    ? t('sidebar.ownership.addOwnersTitle')
                    : t('sidebar.ownership.removeOwnersTitle'))
            }
            open
            onCancel={onModalClose}
            keyboard
            footer={
                <>
                    <Button onClick={onModalClose} type="text">
                        {tc('cancel')}
                    </Button>
                    <Button id="addOwnerButton" disabled={selectedActorUrns.length === 0} onClick={onOk}>
                        {tc('done')}
                    </Button>
                </>
            }
            getContainer={getModalDomContainer}
        >
            <Form layout="vertical" colon={false}>
                <Form.Item key="owners" name="owners" label={<Typography.Text strong>{tcl('owner')}</Typography.Text>}>
                    <Typography.Paragraph>{t('sidebar.ownership.findUserOrGroupText')}</Typography.Paragraph>
                    <FormSection>
                        <ActorsSearchSelect
                            selectedActorUrns={selectedActorUrns}
                            onUpdate={handleActorsUpdate}
                            placeholder={t('sidebar.ownership.searchUsersPlaceholder')}
                            width="full"
                            dataTestId="users-group-search"
                        />
                    </FormSection>
                </Form.Item>
                {!hideOwnerType && (
                    <Form.Item label={<Typography.Text strong>{tcl('type')}</Typography.Text>}>
                        <Typography.Paragraph>{t('sidebar.ownership.chooseOwnerTypeText')}</Typography.Paragraph>
                        <Form.Item name="type">
                            {ownershipTypesLoading && <Select />}
                            {!ownershipTypesLoading && (
                                <Select value={selectedOwnerType} onChange={(v) => setSelectedOwnerType(v as string)}>
                                    {ownershipTypes.map((ownershipType: OwnershipTypeEntity | undefined) => {
                                        const ownershipTypeUrn = ownershipType?.urn || '';
                                        const ownershipTypeName = ownershipType?.info?.name || ownershipType?.urn || '';
                                        const ownershipTypeDescription = ownershipType?.info?.description || '';
                                        return (
                                            <Select.Option key={ownershipTypeUrn} value={ownershipTypeUrn}>
                                                <Typography.Text>{ownershipTypeName}</Typography.Text>
                                                <Typography.Paragraph
                                                    style={{ wordWrap: 'break-word', whiteSpace: 'break-spaces' }}
                                                    type="secondary"
                                                >
                                                    {ownershipTypeDescription}
                                                </Typography.Paragraph>
                                            </Select.Option>
                                        );
                                    })}
                                </Select>
                            )}
                        </Form.Item>
                    </Form.Item>
                )}
            </Form>
        </Modal>
    );
};
