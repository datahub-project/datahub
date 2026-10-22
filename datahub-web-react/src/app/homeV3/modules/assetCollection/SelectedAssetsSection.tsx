import { Text } from '@components';
import { X } from '@phosphor-icons/react/dist/csr/X';
import isEqual from 'lodash/isEqual';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import DraggableEntityItem from '@app/homeV3/modules/assetCollection/dragAndDrop/DraggableEntityItem';
import VerticalDragAndDrop from '@app/homeV3/modules/assetCollection/dragAndDrop/VerticalDragAndDrop';
import { EmptyContainer, StyledIcon } from '@app/homeV3/styledComponents';
import { useGetEntities } from '@app/sharedV2/useGetEntities';

import { DataHubPageModuleType, Entity } from '@types';

const SelectedAssetsContainer = styled.div`
    position: relative;
    display: flex;
    flex-direction: column;
    gap: 8px;
    height: 100%;
    max-height: 440px;
    min-height: 0;
`;

const ResultsContainer = styled.div`
    margin: 0 -12px 0 -8px;
    overflow-y: auto;
    scrollbar-gutter: stable;
    min-height: 0;
`;

type Props = {
    selectedAssetUrns: string[];
    setSelectedAssetUrns: React.Dispatch<React.SetStateAction<string[]>>;
};

const SelectedAssetsSection = ({ selectedAssetUrns, setSelectedAssetUrns }: Props) => {
    const { t } = useTranslation('modules');
    const [orderedUrns, setOrderedUrns] = useState(selectedAssetUrns);

    useEffect(() => {
        if (!isEqual(selectedAssetUrns, orderedUrns)) {
            setOrderedUrns(selectedAssetUrns);
        }
    }, [orderedUrns, selectedAssetUrns]);

    const onChangeOrder = (urns: string[]) => {
        setOrderedUrns(urns);
        setSelectedAssetUrns(urns);
    };

    // Cache resolved entities and fetch only new URNs, so the list doesn't empty (flicker) on each change
    const [entitiesMap, setEntitiesMap] = useState<Record<string, Entity>>({});
    const unresolvedUrns = useMemo(
        () => selectedAssetUrns.filter((urn) => !entitiesMap[urn]),
        [selectedAssetUrns, entitiesMap],
    );
    const { entities } = useGetEntities(unresolvedUrns);

    useEffect(() => {
        if (entities.length) {
            setEntitiesMap((prev) => {
                const next = { ...prev };
                entities.forEach((entity) => {
                    next[entity.urn] = entity;
                });
                return next;
            });
        }
    }, [entities]);

    const handleRemoveAsset = (entity: Entity) => {
        const newUrns = selectedAssetUrns.filter((urn) => !(entity.urn === urn));
        setSelectedAssetUrns(newUrns);
    };

    const renderRemoveAsset = (entity: Entity) => {
        return (
            <StyledIcon
                icon={X}
                color="gray"
                size="md"
                onClick={(e) => {
                    e.preventDefault();
                    handleRemoveAsset(entity);
                }}
            />
        );
    };

    let content;
    if (selectedAssetUrns.length > 0) {
        content = selectedAssetUrns
            .map((urn) => entitiesMap[urn])
            .filter(Boolean)
            .map((entity) => (
                <DraggableEntityItem
                    key={entity.urn}
                    entity={entity}
                    customDetailsRenderer={renderRemoveAsset}
                    moduleType={DataHubPageModuleType.AssetCollection}
                />
            ));
    } else {
        content = (
            <EmptyContainer>
                <Text color="gray">{t('assetCollection.noAssetsSelected')}</Text>
            </EmptyContainer>
        );
    }

    return (
        <SelectedAssetsContainer>
            <Text color="gray" weight="bold">
                {t('assetCollection.selectedAssetsHeader')}
            </Text>
            <VerticalDragAndDrop items={orderedUrns} onChange={onChangeOrder}>
                <ResultsContainer data-testid="selected-assets-list">{content}</ResultsContainer>
            </VerticalDragAndDrop>
        </SelectedAssetsContainer>
    );
};

export default SelectedAssetsSection;
