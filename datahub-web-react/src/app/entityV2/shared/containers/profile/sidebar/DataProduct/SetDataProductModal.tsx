import { LoadingOutlined } from '@ant-design/icons';
import { Empty, Select, message } from 'antd';
import { debounce } from 'lodash';
import React, { useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { getParentEntities } from '@app/entityV2/shared/containers/profile/header/getParentEntities';
import { handleBatchError } from '@app/entityV2/shared/utils';
import ContextPath from '@app/previewV2/ContextPath';
import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Modal, Text } from '@src/alchemy-components';
import { ANTD_GRAY } from '@src/app/entityV2/shared/constants';
import { useGetRecommendations } from '@src/app/shared/recommendation';
import { getModalDomContainer } from '@src/utils/focus';

import { useBatchSetDataProductMutation } from '@graphql/dataProduct.generated';
import { useGetAutoCompleteMultipleResultsLazyQuery } from '@graphql/search.generated';
import { DataHubPageModuleType, DataProduct, Entity, EntityType } from '@types';

const LoadingWrapper = styled.div`
    display: flex;
    justify-content: center;
    margin: 5px;
`;

interface Props {
    urns: string[];
    currentDataProduct: DataProduct | null;
    onModalClose: () => void;
    titleOverride?: string;
    onOkOverride?: (result: string) => void;
    setDataProduct?: (dataProduct: DataProduct | null) => void;
    refetch?: () => void;
}

export default function SetDataProductModal({
    urns,
    currentDataProduct,
    onModalClose,
    titleOverride,
    onOkOverride,
    setDataProduct,
    refetch,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const { reloadByKeyType } = useReloadableContext();
    const [batchSetDataProductMutation] = useBatchSetDataProductMutation();
    const [selectedDataProduct, setSelectedDataProduct] = useState<DataProduct | null>(currentDataProduct);
    const inputEl = useRef(null);

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

    function onOk() {
        if (!selectedDataProduct) return;

        if (onOkOverride) {
            onOkOverride(selectedDataProduct?.urn);
            return;
        }

        batchSetDataProductMutation({
            variables: {
                input: {
                    resourceUrns: urns,
                    dataProductUrn: selectedDataProduct.urn,
                },
            },
        })
            .then(() => {
                message.success({ content: 'Updated Data Product!', duration: 3 });
                setDataProduct?.(selectedDataProduct);
                sendAnalytics();
                onModalClose();
                setSelectedDataProduct(null);
                // refetch is for search results, need to set a timeout
                setTimeout(() => {
                    refetch?.();
                    // Reload modules
                    // Assets - as assets module on data product summary tab could be updated
                    reloadByKeyType([
                        getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.Assets),
                    ]);
                }, 2000);
            })
            .catch((e) => {
                message.destroy();
                message.error(
                    handleBatchError(urns, e, {
                        content: `Failed to add assets to Data Product: \n ${e.message || ''}`,
                        duration: 3,
                    }),
                );
            });
    }

    function onSelectDataProduct(urn: string) {
        if (inputEl && inputEl.current) {
            (inputEl.current as any).blur();
        }
        const dataProduct = displayedDataProducts?.find((entity) => entity.urn === urn);
        setSelectedDataProduct((dataProduct as DataProduct) || null);
    }

    function onDeselect() {
        setSelectedDataProduct(null);
    }

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#setDataProductButton',
    });

    const selectValue =
        (selectedDataProduct && [entityRegistry.getDisplayName(EntityType.DataProduct, selectedDataProduct)]) ||
        undefined;

    const loadingOption = {
        label: (
            <LoadingWrapper>
                <LoadingOutlined />
            </LoadingWrapper>
        ),
        value: 'loading',
    };

    const options = displayedDataProducts.map((result) => {
        return {
            label: (
                <>
                    <Text size="md">{entityRegistry.getDisplayName(EntityType.DataProduct, result)}</Text>
                    <ContextPath
                        entityType={EntityType.DataProduct}
                        displayedEntityType="Data product"
                        parentEntities={getParentEntities(result as DataProduct, EntityType.DataProduct)}
                        entityTitleWidth={200}
                        numVisible={3}
                    />
                </>
            ),
            value: result.urn,
        };
    });

    return (
        <Modal
            title={titleOverride || 'Set Data Product'}
            open
            onCancel={onModalClose}
            getContainer={getModalDomContainer}
            buttons={[
                {
                    text: 'Cancel',
                    variant: 'text',
                    onClick: onModalClose,
                },
                {
                    text: 'Save',
                    variant: 'filled',
                    disabled: !selectedDataProduct,
                    onClick: onOk,
                    id: 'setDataProductButton',
                },
            ]}
        >
            <Select
                autoFocus
                showSearch
                defaultOpen
                filterOption={false}
                defaultActiveFirstOption={false}
                placeholder="Search for Data Products..."
                onSelect={(urn: string) => onSelectDataProduct(urn)}
                onDeselect={onDeselect}
                onSearch={handleSearch}
                style={{ width: '100%' }}
                ref={inputEl}
                value={selectValue}
                options={loading ? [loadingOption] : options}
                notFoundContent={
                    !loading ? (
                        <Empty
                            description="No Data Products Found"
                            image={Empty.PRESENTED_IMAGE_SIMPLE}
                            style={{ color: ANTD_GRAY[7] }}
                        />
                    ) : null
                }
            />
        </Modal>
    );
}
