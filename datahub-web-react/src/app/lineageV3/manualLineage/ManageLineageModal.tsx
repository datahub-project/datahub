import { Modal as AntModal, message } from 'antd';
import React, { useContext, useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { useUserContext } from '@app/context/useUserContext';
import { useOnClickExpandLineage } from '@app/lineageV3/LineageEntityNode/useOnClickExpandLineage';
import { FetchStatus, LineageEntity, LineageNodesContext } from '@app/lineageV3/common';
import LineageEdges from '@app/lineageV3/manualLineage/LineageEdges';
import { buildUpdateLineagePayload } from '@app/lineageV3/manualLineage/buildUpdateLineagePayload';
import { recordAnalyticsEvents } from '@app/lineageV3/manualLineage/recordManualLineageAnalyticsEvent';
import updateNodeContext from '@app/lineageV3/manualLineage/updateNodeContext';
import { getValidEntityTypes } from '@app/lineageV3/manualLineage/utils';
import { useEntityRegistryV2 as useEntityRegistry } from '@app/useEntityRegistry';
import { Modal } from '@src/alchemy-components';
import { EntityAndType } from '@src/app/entity/shared/types';
import { extractTypeFromUrn } from '@src/app/entity/shared/utils';
import { SearchSelect } from '@src/app/entityV2/shared/components/styled/search/SearchSelect';
import ClickOutside from '@src/app/shared/ClickOutside';

import { useUpdateLineageMutation } from '@graphql/mutations.generated';
import { Entity, EntityType, LineageDirection } from '@types';

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
    border-left: 1px solid ${(props) => props.theme.colors.border};
    display: flex;
    flex-direction: column;
`;

const SectionHeader = styled.div`
    padding-left: 20px;
    margin-top: 10px;
    font-size: 16px;
    font-weight: 500;
    color: ${(props) => props.theme.colors.text};
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
    const { t } = useTranslation('lineage');
    const { t: tcAction } = useTranslation('common.actions');
    const { t: tcFeedback } = useTranslation('common.feedback');
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
        .map((urn) => ({ urn }) as Entity);

    // save lineage changes will disable the button while its processing
    function saveLineageChanges() {
        setIsSaving(true);
        const payload = buildUpdateLineagePayload(direction, entitiesToAdd, entitiesToRemove, node.urn);

        updateLineage({ variables: { input: payload } })
            .then((res) => {
                if (res.data?.updateLineage) {
                    closeModal();
                    message.success(t('manualLineage.updateLineageSuccess'));
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
                message.error(error.message || t('manualLineage.updateLineageError'));
                setIsSaving(false);
            });
    }

    const setDirectionsText =
        direction === LineageDirection.Upstream ? t('manualLineage.setUpstreams') : t('manualLineage.setDownstreams');

    const onCancelSelect = () => {
        if (entitiesToAdd.length > 0 || entitiesToRemove.length > 0) {
            AntModal.confirm({
                title: t('manualLineage.exitConfirmTitle'),
                content: t('manualLineage.exitConfirmText', {
                    count: entitiesToAdd.length + entitiesToRemove.length,
                }),
                onOk() {
                    closeModal();
                },
                onCancel() {},
                okText: tcAction('yes'),
                maskClosable: true,
                closable: true,
            });
        } else {
            closeModal();
        }
    };

    const modalButtons = useMemo(
        () => [
            {
                text: tcAction('cancel'),
                variant: 'text' as const,
                onClick: onCancelSelect,
                key: 'cancel',
            },
            {
                text: isSaving ? tcFeedback('saving') : setDirectionsText,
                onClick: saveLineageChanges,
                disabled: (entitiesToAdd.length === 0 && entitiesToRemove.length === 0) || isSaving,
                key: 'save',
            },
        ],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [isSaving, setDirectionsText, entitiesToAdd.length, entitiesToRemove.length],
    );

    return (
        <ClickOutside onClickOutside={onCancelSelect} wrapperClassName="search-select-modal">
            <StyledModal
                title={
                    direction === LineageDirection.Upstream
                        ? t('manualLineage.modalTitleUpstream', { entityName: node.entity?.name })
                        : t('manualLineage.modalTitleDownstream', { entityName: node.entity?.name })
                }
                width={MODAL_WIDTH_PX}
                open
                onCancel={onCancelSelect}
                style={{ padding: 0 }}
                zIndex={2000} // Over node tooltips
                buttons={modalButtons}
            >
                <ModalContentContainer>
                    <SearchSection>
                        <SectionHeader>{t('manualLineage.searchAndAddHeader')}</SectionHeader>
                        <ScrollableContent>
                            <SearchSelect
                                fixedEntityTypes={Array.from(validEntityTypes)}
                                selectedEntities={selectedEntities}
                                setSelectedEntities={setSelectedEntities}
                            />
                        </ScrollableContent>
                    </SearchSection>
                    <CurrentSection>
                        <SectionHeader>
                            {direction === LineageDirection.Upstream
                                ? t('manualLineage.currentUpstreamsHeader')
                                : t('manualLineage.currentDownstreamsHeader')}
                        </SectionHeader>
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
