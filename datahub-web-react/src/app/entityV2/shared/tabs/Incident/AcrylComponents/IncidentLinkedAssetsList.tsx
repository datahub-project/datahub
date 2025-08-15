import { LoadingOutlined } from '@ant-design/icons';
import { Plus } from 'phosphor-react';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { SearchSelectModal } from '@app/entityV2/shared/components/styled/search/SearchSelectModal';
import {
    AssetWrapper,
    LinkedAssets,
    LoadingWrapper,
} from '@app/entityV2/shared/tabs/Incident/AcrylComponents/styledComponents';
import { IncidentAction } from '@app/entityV2/shared/tabs/Incident/constant';
import { LinkedAssetsContainer } from '@app/entityV2/shared/tabs/Incident/styledComponents';
import { IncidentLinkedAssetsListProps } from '@app/entityV2/shared/tabs/Incident/types';
import { Button, Pill } from '@src/alchemy-components';
import { EntityCapabilityType } from '@src/app/entityV2/Entity';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useGetEntitiesLazyQuery } from '@src/graphql/entity.generated';

const RESOURCE_URN_FIELD_NAME = 'resourceUrns';

const StyledButton = styled(Button)`
    padding: 4px;
    margin-top: 8px;
`;

export const IncidentLinkedAssetsList = ({
    initialUrn,
    form,
    data,
    mode,
    setCachedLinkedAssets,
    setIsLinkedAssetsLoading,
}: IncidentLinkedAssetsListProps) => {
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
        const selectedAssets = linkedAssets?.filter((existingAsset: any) => existingAsset.urn !== asset.urn);
        setLinkedAssets(selectedAssets as any);
    };

    const batchAddAssets = (entityUrns: Array<string>) => {
        const existingUrns = form.getFieldValue(RESOURCE_URN_FIELD_NAME) || [];
        const updatedUrns = [...existingUrns, ...entityUrns];
        const uniqueUrns = [...new Set(updatedUrns)];
        form.setFieldValue(RESOURCE_URN_FIELD_NAME, uniqueUrns);
        getEntities({
            variables: { urns: form.getFieldValue(RESOURCE_URN_FIELD_NAME) },
        });
        setIsBatchAddAssetListModalVisible(false);
    };

    useEffect(() => {
        if (mode === IncidentAction.CREATE && initialUrn) {
            if (initialUrn) {
                getEntities({
                    variables: {
                        urns: [initialUrn],
                    },
                });
            }
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    useEffect(() => {
        setLinkedAssets(resolvedLinkedAssets?.entities as any);
        if (mode === IncidentAction.CREATE) {
            form.setFieldValue(RESOURCE_URN_FIELD_NAME, [initialUrn]);
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
                                            label={entityRegistry.getDisplayName(asset.type, asset)}
                                            rightIcon="Close"
                                            color="violet"
                                            variant="outline"
                                            onClickRightIcon={() => {
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
