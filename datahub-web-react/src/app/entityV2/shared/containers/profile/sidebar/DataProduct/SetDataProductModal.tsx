import { Loader, Modal, SimpleSelect, Text, toast } from '@components';
import debounce from 'lodash/debounce';
import React, { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import { getParentEntities } from '@app/entityV2/shared/containers/profile/header/getParentEntities';
import { handleBatchError } from '@app/entityV2/shared/utils';
import ContextPath from '@app/previewV2/ContextPath';
import { useIsMultipleDataProductsEnabled } from '@app/shared/hooks/useIsMultipleDataProductsEnabled';
import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { SelectOption } from '@src/alchemy-components/components/Select/types';
import { useGetRecommendations } from '@src/app/shared/recommendation';
import { getModalDomContainer } from '@src/utils/focus';

import { useBatchAddToDataProductsMutation, useBatchSetDataProductMutation } from '@graphql/dataProduct.generated';
import { useGetAutoCompleteMultipleResultsLazyQuery } from '@graphql/search.generated';
import { DataHubPageModuleType, DataProduct, Entity, EntityType } from '@types';

const LoadingWrapper = styled.div`
    display: flex;
    justify-content: center;
    margin: 5px;
`;

const OptionContent = styled.div`
    display: flex;
    flex-direction: column;
`;

interface Props {
    urns: string[];
    currentDataProducts: DataProduct[];
    onModalClose: () => void;
    titleOverride?: string;
    onOkOverride?: (result: string) => void;
    setDataProducts?: (dataProducts: DataProduct[]) => void;
}

interface DataProductOption extends SelectOption {
    entity?: DataProduct;
}

export default function SetDataProductModal({
    urns,
    currentDataProducts,
    onModalClose,
    titleOverride,
    onOkOverride,
    setDataProducts,
}: Props) {
    const { t } = useTranslation('entity.shared.containers');
    const { t: tc } = useTranslation('common.actions');
    const entityRegistry = useEntityRegistry();
    const { reloadByKeyType, bypassCacheForUrn } = useReloadableContext();
    const isMultipleDataProductsEnabled = useIsMultipleDataProductsEnabled();
    const [batchSetDataProductMutation] = useBatchSetDataProductMutation();
    const [batchAddToDataProductsMutation] = useBatchAddToDataProductsMutation();
    const [selectedDataProducts, setSelectedDataProducts] = useState<DataProduct[]>(
        isMultipleDataProductsEnabled ? [] : currentDataProducts,
    );
    const { isInFormContext } = useEntityFormContext();

    const [getSearchResults, { data, loading: searchLoading }] = useGetAutoCompleteMultipleResultsLazyQuery();
    const { recommendedData: recommendedDataProducts, loading: recommendationsLoading } = useGetRecommendations([
        EntityType.DataProduct,
    ]);
    const [showRecommendations, setShowRecommendations] = useState(true);
    const loading = recommendationsLoading || searchLoading;

    const displayedDataProducts: Entity[] =
        !showRecommendations && data?.autoCompleteForMultiple?.suggestions
            ? data?.autoCompleteForMultiple?.suggestions?.flatMap((suggestion) => suggestion.entities)
            : recommendedDataProducts;

    const handleSearch = useMemo(() => {
        const fetch = (text: string) => {
            if (text.trim()) {
                getSearchResults({
                    variables: {
                        input: {
                            types: [EntityType.DataProduct],
                            query: text.trim(),
                            limit: 10,
                        },
                    },
                });
            }
            setShowRecommendations(!text.trim());
        };
        return debounce(fetch, 100);
    }, [getSearchResults]);

    const sendAnalytics = () => {
        const isBatchAction = urns.length > 1;

        if (isBatchAction) {
            analytics.event({
                type: EventType.BatchEntityActionEvent,
                actionType: EntityActionType.SetDataProduct,
                entityUrns: urns,
            });
        } else {
            analytics.event({
                type: EventType.EntityActionEvent,
                actionType: EntityActionType.SetDataProduct,
                entityUrn: urns[0],
            });
        }
    };

    const handleMutationSuccess = (successMessage: string) => {
        toast.success(successMessage, { duration: 3 });
        if (isMultipleDataProductsEnabled) {
            // Combine with current data products to correcly show them together in entity sidebar
            const existingDataProductUrns = currentDataProducts.map((dp) => dp.urn);
            setDataProducts?.([
                ...currentDataProducts,
                ...selectedDataProducts.filter((dp) => !existingDataProductUrns.includes(dp.urn)),
            ]);
        } else {
            setDataProducts?.(selectedDataProducts);
        }
        sendAnalytics();
        onModalClose();
        setSelectedDataProducts([]);
        urns.forEach((urn) => {
            bypassCacheForUrn(urn);
        });
        reloadByKeyType([getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.Assets)], 3000);
    };

    const handleMutationError = (e: any, errorMessage: string) => {
        toast.destroy();
        const { content, duration } = handleBatchError(urns, e, {
            content: `${errorMessage} \n ${e.message || ''}`,
            duration: 3,
        });
        toast.error(content, { duration });
    };

    function onOk() {
        if (selectedDataProducts.length === 0) return;

        if (onOkOverride) {
            onOkOverride(selectedDataProducts[0]?.urn);
            return;
        }

        if (isMultipleDataProductsEnabled) {
            const dataProductUrns = selectedDataProducts.map((dp) => dp.urn);
            batchAddToDataProductsMutation({
                variables: {
                    input: {
                        resourceUrns: urns,
                        dataProductUrns,
                    },
                },
            })
                .then(() =>
                    handleMutationSuccess(t('sidebar.dataProduct.updatedSuccess', { context: 'multiProducts' })),
                )
                .catch((e) => handleMutationError(e, t('sidebar.dataProduct.addToProductsFailedPrefix')));
        } else {
            batchSetDataProductMutation({
                variables: {
                    input: {
                        resourceUrns: urns,
                        dataProductUrn: selectedDataProducts[0].urn,
                    },
                },
            })
                .then(() => handleMutationSuccess(t('sidebar.dataProduct.updatedSuccess')))
                .catch((e) => handleMutationError(e, t('sidebar.dataProduct.addToProductFailedPrefix')));
        }
    }

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#setDataProductButton',
    });

    const options: DataProductOption[] = useMemo(
        () =>
            displayedDataProducts.map((result) => ({
                value: result.urn,
                label: entityRegistry.getDisplayName(EntityType.DataProduct, result),
                entity: result as DataProduct,
            })),
        [displayedDataProducts, entityRegistry],
    );

    const selectedOptions: DataProductOption[] = useMemo(() => {
        const byUrn = new Map(options.map((option) => [option.value, option]));
        selectedDataProducts.forEach((dp) => {
            if (!byUrn.has(dp.urn)) {
                byUrn.set(dp.urn, {
                    value: dp.urn,
                    label: entityRegistry.getDisplayName(EntityType.DataProduct, dp),
                    entity: dp,
                });
            }
        });
        return Array.from(byUrn.values());
    }, [options, selectedDataProducts, entityRegistry]);

    const values = selectedDataProducts.map((dp) => dp.urn);

    return (
        <Modal
            title={
                titleOverride ||
                t('sidebar.dataProduct.setModalTitle', {
                    context: isMultipleDataProductsEnabled ? 'multiProducts' : undefined,
                })
            }
            open
            onCancel={onModalClose}
            getContainer={!isInFormContext ? getModalDomContainer : undefined} // if filling out form in full page modal, don't change container as this modal gets hidden
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onModalClose,
                },
                {
                    text: tc('save'),
                    variant: 'filled',
                    disabled: selectedDataProducts.length === 0,
                    onClick: onOk,
                    id: 'setDataProductButton',
                },
            ]}
        >
            <SimpleSelect
                showSearch
                defaultOpen
                filterResultsByQuery={false}
                isMultiSelect={isMultipleDataProductsEnabled}
                placeholder={t('sidebar.dataProduct.searchPlaceholder')}
                values={values}
                onUpdate={(next) => {
                    if (!isMultipleDataProductsEnabled) {
                        const urn = next[0];
                        const match = selectedOptions.find((option) => option.value === urn)?.entity;
                        setSelectedDataProducts(match ? [match] : []);
                        return;
                    }
                    const nextProducts = next
                        .map((urn) => selectedOptions.find((option) => option.value === urn)?.entity)
                        .filter((entity): entity is DataProduct => !!entity);
                    setSelectedDataProducts(nextProducts);
                }}
                onSearchChange={handleSearch}
                onClear={() => setSelectedDataProducts([])}
                options={options}
                combinedSelectedAndSearchOptions={selectedOptions}
                isLoading={loading}
                width="full"
                showClear
                renderCustomOptionText={(option) => (
                    <OptionContent>
                        <Text size="md">{option.label}</Text>
                        {option.entity && (
                            <ContextPath
                                entityType={EntityType.DataProduct}
                                displayedEntityType={t('sidebar.dataProduct.entityTypeName')}
                                parentEntities={getParentEntities(option.entity, EntityType.DataProduct)}
                                entityTitleWidth={200}
                                numVisible={3}
                            />
                        )}
                    </OptionContent>
                )}
                emptyState={
                    loading ? (
                        <LoadingWrapper>
                            <Loader size="sm" />
                        </LoadingWrapper>
                    ) : (
                        <Text size="sm" color="textSecondary">
                            {t('sidebar.dataProduct.emptyText')}
                        </Text>
                    )
                }
            />
        </Modal>
    );
}
