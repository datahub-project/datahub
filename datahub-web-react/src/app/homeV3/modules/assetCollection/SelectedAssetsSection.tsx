import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import DraggableEntityItem from '@app/homeV3/modules/assetCollection/dragAndDrop/DraggableEntityItem';
import VerticalDragAndDrop from '@app/homeV3/modules/assetCollection/dragAndDrop/VerticalDragAndDrop';
import { EmptyContainer, StyledIcon } from '@app/homeV3/styledComponents';
import { useGetEntities } from '@app/sharedV2/useGetEntities';

import { Entity } from '@types';

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
    const { entities, loading } = useGetEntities(selectedAssetUrns);

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
    if (entities && entities.length > 0) {
        content = entities.map((entity) => (
            <DraggableEntityItem entity={entity} key={entity.urn} customDetailsRenderer={renderRemoveAsset} />
        ));
    } else if (!loading && entities.length === 0) {
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
            <VerticalDragAndDrop items={selectedAssetUrns} onChange={setSelectedAssetUrns}>
                <ResultsContainer>{content}</ResultsContainer>
            </VerticalDragAndDrop>
        </SelectedAssetsContainer>
    );
};

export default SelectedAssetsSection;
