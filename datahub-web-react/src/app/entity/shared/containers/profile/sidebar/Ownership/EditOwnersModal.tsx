import React, { useEffect, useRef, useState } from 'react';
import { Button, Form, message, Modal, Select, Tag, Typography } from 'antd';
import styled from 'styled-components';

import { CorpUser, Entity, EntityType, OwnerEntityType, OwnershipType } from '../../../../../../../types.generated';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import analytics, { EventType, EntityActionType } from '../../../../../../analytics';
import { OWNERSHIP_DISPLAY_TYPES } from './ownershipUtils';
import {
    useBatchAddOwnersMutation,
    useBatchRemoveOwnersMutation,
} from '../../../../../../../graphql/mutations.generated';
import { useGetSearchResultsLazyQuery } from '../../../../../../../graphql/search.generated';
import { useGetRecommendations } from '../../../../../../shared/recommendation';
import { OwnerLabel } from '../../../../../../shared/OwnerLabel';

const SelectInput = styled(Select)`
    > .ant-select-selector {
        height: 36px;
    }
`;

const StyleTag = styled(Tag)`
    padding: 0px 7px 0px 0px;
    margin-right: 3px;
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
    defaultOwnerType?: OwnershipType;
    hideOwnerType?: boolean | undefined;
    operationType?: OperationType;
    onCloseModal: () => void;
    refetch?: () => Promise<any>;
    entityType?: EntityType; // Only used for tracking events
    onOkOverride?: (result: SelectedOwner[]) => void;
    title?: string;
    defaultValues?: { urn: string; entity?: Entity | null }[];
};

// value: {ownerUrn: string, ownerEntityType: EntityType}
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
        const displayName = entityRegistry.getDisplayName(entity.type, entity);
        return (
            <Select.Option value={entity.urn} key={entity.urn}>
                <OwnerLabel name={displayName} avatarUrl={avatarUrl} type={entity.type} />
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
    const ownershipTypes = OWNERSHIP_DISPLAY_TYPES;
    const [selectedOwners, setSelectedOwners] = useState<SelectedOwner[]>(
        defaultValuesToSelectedOwners(defaultValues || []),
    );
    const [selectedOwnerType, setSelectedOwnerType] = useState<OwnershipType>(defaultOwnerType || OwnershipType.None);

    // User and group dropdown search results!
    const [userSearch, { data: userSearchData }] = useGetSearchResultsLazyQuery();
    const [groupSearch, { data: groupSearchData }] = useGetSearchResultsLazyQuery();
    const userSearchResults = userSearchData?.search?.searchResults?.map((searchResult) => searchResult.entity) || [];
    const groupSearchResults = groupSearchData?.search?.searchResults?.map((searchResult) => searchResult.entity) || [];
    const combinedSearchResults = [...userSearchResults, ...groupSearchResults];
    const [recommendedData] = useGetRecommendations([EntityType.CorpGroup, EntityType.CorpUser]);
    const inputEl = useRef(null);

    useEffect(() => {
        if (ownershipTypes) {
            setSelectedOwnerType(ownershipTypes[0].type);
        }
    }, [ownershipTypes]);

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
        setSelectedOwnerType(defaultOwnerType || OwnershipType.None);
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
    const onSelectOwnerType = (newType: OwnershipType) => {
        setSelectedOwnerType(newType);
    };

    const tagRender = (props) => {
        // eslint-disable-next-line react/prop-types
        const { label, closable, onClose } = props;
        const onPreventMouseDown = (event) => {
            event.preventDefault();
            event.stopPropagation();
        };
        return (
            <StyleTag onMouseDown={onPreventMouseDown} closable={closable} onClose={onClose}>
                {label}
            </StyleTag>
        );
    };

    const emitAnalytics = async () => {
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
                message.error({ content: `Failed to add owners: \n ${e.message || ''}`, duration: 3 });
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
                message.error({ content: `Failed to remove owners: \n ${e.message || ''}`, duration: 3 });
            }
        } finally {
            refetch?.();
            onModalClose();
        }
    };

    // Function to handle the modal action's
    const onOk = async () => {
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
                type: selectedOwnerType,
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
            visible
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
                        >
                            {ownerSearchOptions}
                        </SelectInput>
                    </Form.Item>
                </Form.Item>
                {!hideOwnerType && (
                    <Form.Item label={<Typography.Text strong>Type</Typography.Text>}>
                        <Typography.Paragraph>Choose an owner type</Typography.Paragraph>
                        <Form.Item name="type">
                            <Select
                                defaultValue={selectedOwnerType}
                                value={selectedOwnerType}
                                onChange={onSelectOwnerType}
                            >
                                {ownershipTypes.map((ownerType) => (
                                    <Select.Option key={ownerType.type} value={ownerType.type}>
                                        <Typography.Text>{ownerType.name}</Typography.Text>
                                        <div>
                                            <Typography.Paragraph style={{ wordBreak: 'break-all' }} type="secondary">
                                                {ownerType.description}
                                            </Typography.Paragraph>
                                        </div>
                                    </Select.Option>
                                ))}
                            </Select>
                        </Form.Item>
                    </Form.Item>
                )}
            </Form>
        </Modal>
    );
};
