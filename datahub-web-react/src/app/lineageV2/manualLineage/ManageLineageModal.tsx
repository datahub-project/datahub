import { colors, Modal } from '@src/alchemy-components';
import { EntityAndType } from '@src/app/entity/shared/types';
import { extractTypeFromUrn } from '@src/app/entity/shared/utils';
import { SearchSelect } from '@src/app/entityV2/shared/components/styled/search/SearchSelect';
import ClickOutside from '@src/app/shared/ClickOutside';
import { Modal as AntModal, message } from 'antd';
import React, { useContext, useEffect, useState } from 'react';
import styled from 'styled-components/macro';
import { toTitleCase } from '../../../graphql-mock/helper';
import { useUpdateLineageMutation } from '../../../graphql/mutations.generated';
import { Entity, EntityType, LineageDirection } from '../../../types.generated';
import { useUserContext } from '../../context/useUserContext';
import { useEntityRegistryV2 as useEntityRegistry } from '../../useEntityRegistry';
import { useOnClickExpandLineage } from '../LineageEntityNode/useOnClickExpandLineage';
import { FetchStatus, LineageEntity, LineageNodesContext } from '../common';
import LineageEdges from './LineageEdges';
import { buildUpdateLineagePayload } from './buildUpdateLineagePayload';
import { recordAnalyticsEvents } from './recordManualLineageAnalyticsEvent';
import updateNodeContext from './updateNodeContext';
import { getValidEntityTypes } from './utils';

const MODAL_WIDTH_PX = 1400;

const StyledModal = styled(Modal)`
    top: 30px;
    padding: 0;
`;

const ModalContentContainer = styled.div`
    height: 75vh;
    margin: -24px -20px;
    display: flex;
    flex-direction: row;
`;

const SearchSection = styled.div`
    flex: 2;
    height: 100%;
    overflow: hidden;
    display: flex;
    flex-direction: column;
`;

const CurrentSection = styled.div`
    flex: 1;
    width: 40%;
    border-left: 1px solid ${colors.gray[100]};
    display: flex;
    flex-direction: column;
`;

const SectionHeader = styled.div`
    padding-left: 20px;
    margin-top: 10px;
    font-size: 16px;
    font-weight: 500;
    color: ${colors.gray[600]};
`;

const ScrollableContent = styled.div`
    flex: 1;
    overflow: auto;
`;

interface Props {
    node: LineageEntity;
    direction: LineageDirection;
    closeModal: () => void;
    refetch?: () => void;
}

export default function ManageLineageModal({ node, direction, closeModal, refetch }: Props) {
    const nodeContext = useContext(LineageNodesContext);
    const expandOneLevel = useOnClickExpandLineage(node.urn, node.type, direction, false);
    const { user } = useUserContext();
    const entityRegistry = useEntityRegistry();
    const [updateLineage] = useUpdateLineageMutation();
    const fetchStatus = node.fetchStatus[direction];
    const { adjacencyList } = nodeContext;
    const validEntityTypes = getValidEntityTypes(direction, node.entity?.type);
    const initialSetOfRelationshipsUrns = adjacencyList[direction].get(node.urn) || new Set();
    const [isSaving, setIsSaving] = useState(false);

    const [selectedEntities, setSelectedEntities] = useState<EntityAndType[]>(
        Array.from(initialSetOfRelationshipsUrns).map((urn) => ({ urn, type: extractTypeFromUrn(urn) })) || [],
    );

    useEffect(() => {
        if (fetchStatus === FetchStatus.UNFETCHED) {
            expandOneLevel();
        }
    }, [fetchStatus, expandOneLevel]);

    const entitiesToAdd = selectedEntities.filter((entity) => !initialSetOfRelationshipsUrns.has(entity.urn));
    const entitiesToRemove = Array.from(initialSetOfRelationshipsUrns)
        .filter((urn) => !selectedEntities.map((entity) => entity.urn).includes(urn))
        .map((urn) => ({ urn } as Entity));

    // save lineage changes will disable the button while its processing
    function saveLineageChanges() {
        setIsSaving(true);
        const payload = buildUpdateLineagePayload(direction, entitiesToAdd, entitiesToRemove, node.urn);

        updateLineage({ variables: { input: payload } })
            .then((res) => {
                if (res.data?.updateLineage) {
                    closeModal();
                    message.success('Updated lineage, refetching graph!');
                    updateNodeContext(node.urn, direction, user, nodeContext, entitiesToAdd, entitiesToRemove);
                    refetch?.();

                    recordAnalyticsEvents({
                        direction,
                        entitiesToAdd,
                        entitiesToRemove,
                        entityRegistry,
                        entityType: node.type,
                        entityPlatform: entityRegistry.getDisplayName(EntityType.DataPlatform, node.entity?.platform),
                    });
                    setIsSaving(false);
                }
            })
            .catch((error) => {
                message.error(error.message || 'Error updating lineage');
                setIsSaving(false);
            });
    }

    const directionTitle = toTitleCase(direction.toLocaleLowerCase());

    const onCancelSelect = () => {
        if (entitiesToAdd.length > 0 || entitiesToRemove.length > 0) {
            AntModal.confirm({
                title: `Exit Lineage Management`,
                content: `Are you sure you want to exit? ${
                    entitiesToAdd.length + entitiesToRemove.length
                } change(s) will be cleared.`,
                onOk() {
                    closeModal();
                },
                onCancel() {},
                okText: 'Yes',
                maskClosable: true,
                closable: true,
            });
        } else {
            closeModal();
        }
    };

    return (
        <ClickOutside onClickOutside={onCancelSelect} wrapperClassName="search-select-modal">
            <StyledModal
                title={`Select the ${directionTitle}s to add to ${node.entity?.name}`}
                width={MODAL_WIDTH_PX}
                open
                onCancel={onCancelSelect}
                style={{ padding: 0 }}
                buttons={[
                    {
                        text: 'Cancel',
                        variant: 'text',
                        onClick: onCancelSelect,
                    },
                    {
                        text: isSaving ? 'Saving...' : `Set ${directionTitle}s`,
                        onClick: saveLineageChanges,
                        disabled: (entitiesToAdd.length === 0 && entitiesToRemove.length === 0) || isSaving,
                    },
                ]}
            >
                <ModalContentContainer>
                    <SearchSection>
                        <SectionHeader>Search and Add</SectionHeader>
                        <ScrollableContent>
                            <SearchSelect
                                fixedEntityTypes={Array.from(validEntityTypes)}
                                selectedEntities={selectedEntities}
                                setSelectedEntities={setSelectedEntities}
                            />
                        </ScrollableContent>
                    </SearchSection>
                    <CurrentSection>
                        <SectionHeader>Current {directionTitle}s</SectionHeader>
                        <ScrollableContent>
                            <LineageEdges
                                parentUrn={node.urn}
                                direction={direction}
                                entitiesToAdd={entitiesToAdd}
                                entitiesToRemove={entitiesToRemove}
                                onRemoveEntity={(entity) => {
                                    setSelectedEntities(selectedEntities.filter((e) => e.urn !== entity.urn));
                                }}
                            />
                        </ScrollableContent>
                    </CurrentSection>
                </ModalContentContainer>
            </StyledModal>
        </ClickOutside>
    );
}
