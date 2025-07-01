import { Button, Form, Modal, Select, Tag, Typography, message } from 'antd';
import React, { ReactNode, useEffect, useMemo, useRef, useState } from 'react';
import styled from 'styled-components/macro';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { handleBatchError } from '@app/entity/shared/utils';
import { OwnerLabel } from '@app/shared/OwnerLabel';
import { useGetRecommendations } from '@app/shared/recommendation';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { getModalDomContainer } from '@utils/focus';

import { useBatchAddOwnersMutation, useBatchRemoveOwnersMutation } from '@graphql/mutations.generated';
import { useListOwnershipTypesQuery } from '@graphql/ownership.generated';
import { useGetSearchResultsLazyQuery } from '@graphql/search.generated';
import { CorpUser, Entity, EntityType, OwnerEntityType, OwnershipTypeEntity } from '@types';

const SelectInput = styled(Select)`
    width: 480px;
`;

const StyleTag = styled(Tag)`
    padding: 0px 7px 0px 0px;
    margin: 2px;
    display: flex;
    justify-content: start;
    align-items: center;
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
    entityType?: EntityType; // Only used for tracking events
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

    // Renders a search result in the select dropdown.
    const renderSearchResult = (entity: Entity) => {
        const avatarUrl =
            (entity.type === EntityType.CorpUser && (entity as CorpUser).editableProperties?.pictureLink) || undefined;
        const corpUserDepartmentName =
            (entity.type === EntityType.CorpUser && (entity as CorpUser).properties?.departmentName) || '';
        const corpUserId = (entity.type === EntityType.CorpUser && (entity as CorpUser).username) || '';
        const corpUserTitle = (entity.type === EntityType.CorpUser && (entity as CorpUser).properties?.title) || '';
        const displayName = entityRegistry.getDisplayName(entity.type, entity);

        return (
            <Select.Option
                key={entity.urn}
                value={entity.urn}
                label={<OwnerLabel name={displayName} avatarUrl={avatarUrl} type={entity.type} />}
            >
                <OwnerLabel
                    name={displayName}
                    avatarUrl={avatarUrl}
                    type={entity.type}
                    corpUserId={corpUserId}
                    corpUserTitle={corpUserTitle}
                    corpUserDepartmentName={corpUserDepartmentName}
                />
            </Select.Option>
        );
    };

    const renderDropdownResult = (entity: Entity) => {
        const avatarUrl =
            entity.type === EntityType.CorpUser
                ? (entity as CorpUser).editableProperties?.pictureLink || undefined
                : undefined;
        const displayName = entityRegistry.getDisplayName(entity.type, entity);
        return <OwnerLabel name={displayName} avatarUrl={avatarUrl} type={entity.type} />;
    };

    const defaultValuesToSelectedOwners = (vals: { urn: string; entity?: Entity | null }[]): SelectedOwner[] => {
        return vals.map((defaultValue) => ({
            label: defaultValue.entity ? renderDropdownResult(defaultValue.entity) : defaultValue.urn,
            value: {
                ownerUrn: defaultValue.urn,
                ownerEntityType: defaultValue.entity?.type || EntityType.CorpUser,
            },
        }));
    };

    const [inputValue, setInputValue] = useState('');
    const [batchAddOwnersMutation] = useBatchAddOwnersMutation();
    const [batchRemoveOwnersMutation] = useBatchRemoveOwnersMutation();
    const { data: ownershipTypesData, loading } = useListOwnershipTypesQuery({
        variables: {
            input: {},
        },
    });
    const ownershipTypes = useMemo(() => {
        return ownershipTypesData?.listOwnershipTypes?.ownershipTypes || [];
    }, [ownershipTypesData]);

    const [selectedOwners, setSelectedOwners] = useState<SelectedOwner[]>(
        defaultValuesToSelectedOwners(defaultValues || []),
    );
    const [selectedOwnerType, setSelectedOwnerType] = useState<string | undefined>(undefined);

    useEffect(() => {
        if (ownershipTypes.length) {
            const defaultType = ownershipTypes.find((type) => type.urn === defaultOwnerType);
            setSelectedOwnerType(defaultType?.urn || ownershipTypes[0].urn);
        }
    }, [ownershipTypes, defaultOwnerType]);

    // User and group dropdown search results!
    const [userSearch, { data: userSearchData }] = useGetSearchResultsLazyQuery();
    const [groupSearch, { data: groupSearchData }] = useGetSearchResultsLazyQuery();
    const userSearchResults: Array<Entity> =
        userSearchData?.search?.searchResults?.map((searchResult) => searchResult.entity) || [];
    const groupSearchResults: Array<Entity> =
        groupSearchData?.search?.searchResults?.map((searchResult) => searchResult.entity) || [];
    const combinedSearchResults = [...userSearchResults, ...groupSearchResults];
    const { recommendedData } = useGetRecommendations([EntityType.CorpGroup, EntityType.CorpUser]);
    const inputEl = useRef(null);

    // Invokes the search API as the owner types
    const handleSearch = (type: EntityType, text: string, searchQuery: any) => {
        searchQuery({
            variables: {
                input: {
                    type,
                    query: text,
                    start: 0,
                    count: 5,
                },
            },
        });
    };

    // Invokes the user search API for both users and groups.
    const handleActorSearch = (text: string) => {
        handleSearch(EntityType.CorpUser, text, userSearch);
        handleSearch(EntityType.CorpGroup, text, groupSearch);
    };

    const ownerResult = !inputValue || inputValue.length === 0 ? recommendedData : combinedSearchResults;

    const ownerSearchOptions = ownerResult?.map((result) => {
        return renderSearchResult(result);
    });

    const onModalClose = () => {
        setInputValue('');
        setSelectedOwners([]);
        setSelectedOwnerType(defaultOwnerType || undefined);
        onCloseModal();
    };

    /**
     * When a owner search result is selected, add the new owner  to the selectedOwners
     * value: {ownerUrn: string, ownerEntityType: EntityType}
     */
    const onSelectOwner = (selectedValue: { key: string; label: React.ReactNode; value: string }) => {
        if (inputEl && inputEl.current) {
            (inputEl.current as any).blur();
        }
        const filteredActors = ownerResult
            ?.filter((entity) => entity.urn === selectedValue.value)
            .map((entity) => entity);
        if (filteredActors?.length) {
            const actor = filteredActors[0];
            const ownerEntityType =
                actor && actor.type === EntityType.CorpGroup ? OwnerEntityType.CorpGroup : OwnerEntityType.CorpUser;
            const newValues = [
                ...selectedOwners,
                {
                    label: selectedValue.value,
                    value: {
                        ownerUrn: selectedValue.value,
                        ownerEntityType: ownerEntityType as unknown as EntityType,
                    },
                },
            ];
            setSelectedOwners(newValues);
        }
    };

    // When a owner search result is deselected, remove the Owner
    const onDeselectOwner = (selectedValue: { key: string; label: React.ReactNode; value: string }) => {
        setInputValue('');
        const newValues = selectedOwners.filter(
            (owner) => owner.label !== selectedValue.value && owner.value.ownerUrn !== selectedValue.value,
        );
        setSelectedOwners(newValues);
    };

    // When a owner type is selected, set the type as selected type.
    const onSelectOwnerType = (urn: string) => {
        setSelectedOwnerType(urn);
    };

    const tagRender = ({ closable, label, onClose }: { closable: boolean; label: ReactNode; onClose: () => void }) => {
        return (
            <StyleTag
                onMouseDown={(event) => {
                    event.preventDefault();
                    event.stopPropagation();
                }}
                closable={closable}
                onClose={onClose}
            >
                {label}
            </StyleTag>
        );
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

    // Function to handle the modal action's
    const onOk = () => {
        if (selectedOwners.length === 0) {
            return;
        }

        if (onOkOverride) {
            onOkOverride(selectedOwners);
            return;
        }

        const inputs = selectedOwners.map((selectedActor) => {
            const input = {
                ownerUrn: selectedActor.value.ownerUrn,
                ownerEntityType: selectedActor.value.ownerEntityType,
                ownershipTypeUrn: selectedOwnerType,
            };
            return input;
        });

        if (operationType === OperationType.ADD) {
            batchAddOwners(inputs);
        } else {
            batchRemoveOwners(inputs);
        }
    };

    function handleBlur() {
        setInputValue('');
    }

    return (
        <Modal
            title={title || `${operationType === OperationType.ADD ? 'Add' : 'Remove'} Owners`}
            open
            onCancel={onModalClose}
            keyboard
            footer={
                <>
                    <Button onClick={onModalClose} type="text">
                        Cancel
                    </Button>
                    <Button id="addOwnerButton" disabled={selectedOwners.length === 0} onClick={onOk}>
                        Done
                    </Button>
                </>
            }
            getContainer={getModalDomContainer}
        >
            <Form layout="vertical" colon={false}>
                <Form.Item key="owners" name="owners" label={<Typography.Text strong>Owner</Typography.Text>}>
                    <Typography.Paragraph>Find a user or group</Typography.Paragraph>
                    <Form.Item name="owner">
                        <SelectInput
                            labelInValue
                            autoFocus
                            defaultOpen
                            mode="multiple"
                            ref={inputEl}
                            placeholder="Search for users or groups..."
                            showSearch
                            filterOption={false}
                            defaultActiveFirstOption={false}
                            onSelect={(asset: any) => onSelectOwner(asset)}
                            onDeselect={(asset: any) => onDeselectOwner(asset)}
                            onSearch={(value: string) => {
                                // eslint-disable-next-line react/prop-types
                                handleActorSearch(value.trim());
                                // eslint-disable-next-line react/prop-types
                                setInputValue(value.trim());
                            }}
                            tagRender={tagRender}
                            onBlur={handleBlur}
                            value={selectedOwners as any}
                            defaultValue={selectedOwners.map((owner) => ({
                                key: owner.value.ownerUrn,
                                value: owner.value.ownerUrn,
                                label: owner.label,
                            }))}
                            optionLabelProp="label"
                            data-testid="users-group-search"
                        >
                            {ownerSearchOptions}
                        </SelectInput>
                    </Form.Item>
                </Form.Item>
                {!hideOwnerType && (
                    <Form.Item label={<Typography.Text strong>Type</Typography.Text>}>
                        <Typography.Paragraph>Choose an owner type</Typography.Paragraph>
                        <Form.Item name="type">
                            {loading && <Select />}
                            {!loading && (
                                <Select value={selectedOwnerType} onChange={onSelectOwnerType}>
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
