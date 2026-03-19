import { Form, Typography, message } from 'antd';
import React, { useCallback, useEffect, useState } from 'react';
import styled from 'styled-components/macro';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { ActorsSearchSelect } from '@app/entityV2/shared/EntitySearchSelect/ActorsSearchSelect';
import OwnershipTypesSelect from '@app/entityV2/shared/containers/profile/sidebar/Ownership/OwnershipTypesSelect';
import { handleBatchError } from '@app/entityV2/shared/utils';
import { ActorEntity } from '@app/entityV2/shared/utils/actorUtils';
import { useOwnershipTypes } from '@app/sharedV2/owners/useOwnershipTypes';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Modal } from '@src/alchemy-components';
import { getModalDomContainer } from '@utils/focus';

import { useBatchAddOwnersMutation, useBatchRemoveOwnersMutation } from '@graphql/mutations.generated';
import { DataHubPageModuleType, Entity, EntityType, OwnerEntityType } from '@types';

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
    const { reloadByKeyType } = useReloadableContext();
    const { user } = useUserContext();

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
            message.success({ content: 'Owners Added', duration: 2 });
            emitAnalytics();
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error(
                    handleBatchError(urns, e, {
                        content: `Failed to add owners: \n ${e.message || ''}`,
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
            message.success({ content: 'Owners Removed', duration: 2 });
            emitAnalytics();
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error(
                    handleBatchError(urns, e, {
                        content: `Failed to remove owners: \n ${e.message || ''}`,
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

        const isCurrentUserUpdated = user?.urn && inputs.map((input) => input.ownerUrn).includes(user?.urn);
        if (isCurrentUserUpdated)
            reloadByKeyType(
                [getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.OwnedAssets)],
                3000,
            );
    };

    return (
        <Modal
            title={title || `${operationType === OperationType.ADD ? 'Add' : 'Remove'} Owners`}
            onCancel={onModalClose}
            keyboard
            buttons={[
                {
                    text: 'Cancel',
                    variant: 'text',
                    onClick: onModalClose,
                },
                {
                    text: 'Add',
                    id: 'addOwnerButton',
                    variant: 'filled',
                    disabled: selectedActorUrns.length === 0,
                    onClick: onOk,
                },
            ]}
            getContainer={getModalDomContainer}
        >
            <Form layout="vertical" colon={false}>
                <Form.Item key="owners" name="owners" label={<Typography.Text strong>Owner</Typography.Text>}>
                    <Typography.Paragraph>Find a user or group</Typography.Paragraph>
                    <FormSection>
                        <ActorsSearchSelect
                            selectedActorUrns={selectedActorUrns}
                            onUpdate={handleActorsUpdate}
                            placeholder="Search for users or groups..."
                            width="full"
                            dataTestId="add-owners-select"
                        />
                    </FormSection>
                </Form.Item>
                {!hideOwnerType && (
                    <Form.Item label={<Typography.Text strong>Type</Typography.Text>}>
                        <Typography.Paragraph>Choose an owner type</Typography.Paragraph>
                        <Form.Item name="type">
                            {ownershipTypesLoading && <div />}
                            {!ownershipTypesLoading && (
                                <OwnershipTypesSelect
                                    selectedOwnerTypeUrn={selectedOwnerType}
                                    ownershipTypes={ownershipTypes}
                                    onSelectOwnerType={(urn: string) => setSelectedOwnerType(urn)}
                                />
                            )}
                        </Form.Item>
                    </Form.Item>
                )}
            </Form>
        </Modal>
    );
};
