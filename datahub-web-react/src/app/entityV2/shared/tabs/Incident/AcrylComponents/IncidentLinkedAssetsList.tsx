import React, { useEffect, useState } from 'react';
import { LoadingOutlined } from '@ant-design/icons';
import { Button, Pill } from '@src/alchemy-components';
import { Plus } from 'phosphor-react';
import styled from 'styled-components';
import { useGetEntitiesLazyQuery } from '@src/graphql/entity.generated';
import { EntityCapabilityType } from '@src/app/entityV2/Entity';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { IncidentLinkedAssetsListProps } from '../types';
import { AssetWrapper, LinkedAssets, LoadingWrapper } from './styledComponents';
import { LinkedAssetsContainer } from '../styledComponents';
import { SearchSelectModal } from '../../../components/styled/search/SearchSelectModal';
import { IncidentAction } from '../constant';

const RESOURCE_URN_FIELD_NAME = 'resourceUrns';

const StyledButton = styled(Button)`
    padding: 4px;
    margin-top: 8px;
`;

export const IncidentLinkedAssetsList = ({
    form,
    data,
    mode,
    setCachedLinkedAssets,
    setIsLinkedAssetsLoading,
}: IncidentLinkedAssetsListProps) => {
    const { urn } = useEntityData();
    const [getEntities, { data: resolvedLinkedAssets, loading: entitiesLoading }] = useGetEntitiesLazyQuery();
    const entityRegistry = useEntityRegistryV2();

    const [linkedAssets, setLinkedAssets] = useState<any[]>(data?.linkedAssets || []);
    const [isBatchAddAssetListModalVisible, setIsBatchAddAssetListModalVisible] = useState(false);

    useEffect(() => {
        form.setFieldValue(
            RESOURCE_URN_FIELD_NAME,
            linkedAssets?.map((asset) => asset?.urn),
        );
    }, [linkedAssets, form]);

    useEffect(() => {
        setCachedLinkedAssets(linkedAssets);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [linkedAssets]);

    const removeLinkedAsset = (asset) => {
        console.log('Removing linked asset ');
        const selectedAssets = linkedAssets?.filter((existingAsset: any) => existingAsset.urn !== asset.urn);
        setLinkedAssets(selectedAssets as any);
    };

    const batchAddAssets = (entityUrns: Array<string>) => {
        const updatedUrns = [...form.getFieldValue(RESOURCE_URN_FIELD_NAME), ...entityUrns];
        const uniqueUrns = [...new Set(updatedUrns)];
        form.setFieldValue(RESOURCE_URN_FIELD_NAME, uniqueUrns);
        setIsBatchAddAssetListModalVisible(false);
        getEntities({
            variables: { urns: form.getFieldValue(RESOURCE_URN_FIELD_NAME) },
        });
    };

    useEffect(() => {
        if (mode === IncidentAction.CREATE) {
            getEntities({
                variables: {
                    urns: [urn],
                },
            });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    useEffect(() => {
        setLinkedAssets(resolvedLinkedAssets?.entities as any);
        if (mode === IncidentAction.CREATE) {
            form.setFieldValue(RESOURCE_URN_FIELD_NAME, [urn]);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [resolvedLinkedAssets]);

    useEffect(() => {
        if (data?.linkedAssets?.length) {
            getEntities({
                variables: {
                    urns: data?.linkedAssets?.map((asset) => asset.urn),
                },
            });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [data]);

    useEffect(() => {
        setIsLinkedAssetsLoading(entitiesLoading);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [entitiesLoading]);

    return (
        <>
            <AssetWrapper>
                {entitiesLoading && (
                    <LoadingWrapper>
                        <LoadingOutlined />
                    </LoadingWrapper>
                )}
                {form.getFieldValue(RESOURCE_URN_FIELD_NAME)?.length > 0 &&
                    (entitiesLoading ? (
                        <LoadingWrapper>
                            <LoadingOutlined />
                        </LoadingWrapper>
                    ) : (
                        <LinkedAssetsContainer>
                            <LinkedAssets data-testid="drawer-incident-linked-assets">
                                {linkedAssets?.map((asset: any) => {
                                    return (
                                        <Pill
                                            key={asset.urn}
                                            label={asset?.properties?.name}
                                            rightIcon="Close"
                                            color="violet"
                                            variant="outline"
                                            onClickRightIcon={() => {
                                                console.log('Clicked to remove');
                                                removeLinkedAsset(asset);
                                            }}
                                            clickable
                                        />
                                    );
                                })}
                            </LinkedAssets>
                        </LinkedAssetsContainer>
                    ))}
                {!entitiesLoading && (
                    <StyledButton
                        variant="outline"
                        type="button"
                        data-testid="add-linked-asset-incident-button"
                        onClick={() => setIsBatchAddAssetListModalVisible(true)}
                    >
                        <Plus />
                    </StyledButton>
                )}
            </AssetWrapper>
            {isBatchAddAssetListModalVisible && (
                <SearchSelectModal
                    titleText="Link assets to incident"
                    continueText="Add"
                    onContinue={batchAddAssets}
                    onCancel={() => setIsBatchAddAssetListModalVisible(false)}
                    fixedEntityTypes={Array.from(
                        entityRegistry.getTypesWithSupportedCapabilities(EntityCapabilityType.GLOSSARY_TERMS),
                    )}
                />
            )}
        </>
    );
};
