import { Text } from '@components';
import { isEqual } from 'lodash';
import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import DraggableEntityItem from '@app/homeV3/modules/assetCollection/dragAndDrop/DraggableEntityItem';
import VerticalDragAndDrop from '@app/homeV3/modules/assetCollection/dragAndDrop/VerticalDragAndDrop';
import { EmptyContainer, StyledIcon } from '@app/homeV3/styledComponents';
import { useGetEntities } from '@app/sharedV2/useGetEntities';

import { DataHubPageModuleType, Entity } from '@types';

const SelectedAssetsContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    height: 100%;
`;

const ResultsContainer = styled.div`
    margin: 0 -12px 0 -8px;
    height: 100%;
`;

type Props = {
    selectedAssetUrns: string[];
    setSelectedAssetUrns: React.Dispatch<React.SetStateAction<string[]>>;
};

const SelectedAssetsSection = ({ selectedAssetUrns, setSelectedAssetUrns }: Props) => {
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

    // To prevent refetch on only order change
    const stableUrns = useMemo(() => [...selectedAssetUrns].sort(), [selectedAssetUrns]);
    const { entities } = useGetEntities(stableUrns);

    const entitiesMap = useMemo(() => {
        const map: Record<string, Entity> = {};
        entities.forEach((entity) => {
            map[entity.urn] = entity;
        });
        return map;
    }, [entities]);

    const handleRemoveAsset = (entity: Entity) => {
        const newUrns = selectedAssetUrns.filter((urn) => !(entity.urn === urn));
        setSelectedAssetUrns(newUrns);
    };

    const renderRemoveAsset = (entity: Entity) => {
        return (
            <StyledIcon
                icon="X"
                source="phosphor"
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
                <Text color="gray">No assets selected.</Text>
            </EmptyContainer>
        );
    }

    return (
        <SelectedAssetsContainer>
            <Text color="gray" weight="bold">
                Selected Assets
            </Text>
            <VerticalDragAndDrop items={orderedUrns} onChange={onChangeOrder}>
                <ResultsContainer>{content}</ResultsContainer>
            </VerticalDragAndDrop>
        </SelectedAssetsContainer>
    );
};

export default SelectedAssetsSection;
