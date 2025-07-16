import { Icon, Loader, Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import EntityItem from '@app/homeV3/module/components/EntityItem';
import { EmptyContainer, LoaderContainer } from '@app/homeV3/styledComponents';
import { useGetEntities } from '@app/sharedV2/useGetEntities';

import { Entity } from '@types';

const SelectedAssetsContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
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
            <Icon
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
    if (loading) {
        content = (
            <LoaderContainer>
                <Loader />
            </LoaderContainer>
        );
    } else if (entities && entities.length > 0) {
        content = entities.map((entity) => (
            <EntityItem entity={entity} key={entity.urn} customDetailsRenderer={renderRemoveAsset} />
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
            {content}
        </SelectedAssetsContainer>
    );
};

export default SelectedAssetsSection;
