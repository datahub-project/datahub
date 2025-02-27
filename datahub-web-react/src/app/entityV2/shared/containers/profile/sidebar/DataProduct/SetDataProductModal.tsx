import { LoadingOutlined } from '@ant-design/icons';
import Modal from 'antd/lib/modal/Modal';
import { Button, Empty, Select, message } from 'antd';
import { getModalDomContainer } from '@src/utils/focus';
import { ANTD_GRAY } from '@src/app/entityV2/shared/constants';
import { debounce } from 'lodash';
import React, { useMemo, useRef, useState } from 'react';
import styled from 'styled-components';
import { useGetRecommendations } from '@src/app/shared/recommendation';
import { useGetAutoCompleteMultipleResultsLazyQuery } from '../../../../../../../graphql/search.generated';
import { DataProduct, Entity, EntityType } from '../../../../../../../types.generated';
import { useEnterKeyListener } from '../../../../../../shared/useEnterKeyListener';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { IconStyleType } from '../../../../../Entity';
import { useBatchSetDataProductMutation } from '../../../../../../../graphql/dataProduct.generated';
import { handleBatchError } from '../../../../utils';

const OptionWrapper = styled.div`
    padding: 2px 0;

    svg {
        margin-right: 8px;
    }
`;

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
                onModalClose();
                setSelectedDataProduct(null);
                // refetch is for search results, need to set a timeout
                setTimeout(() => {
                    refetch?.();
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

    const options = displayedDataProducts.map((result) => ({
        label: (
            <OptionWrapper>
                {entityRegistry.getIcon(EntityType.DataProduct, 12, IconStyleType.ACCENT, 'black')}
                {entityRegistry.getDisplayName(EntityType.DataProduct, result)}
            </OptionWrapper>
        ),
        value: result.urn,
    }));

    return (
        <Modal
            title={titleOverride || 'Set Data Product'}
            open
            onCancel={onModalClose}
            getContainer={getModalDomContainer}
            footer={
                <>
                    <Button onClick={onModalClose} type="text">
                        Cancel
                    </Button>
                    <Button type="primary" id="setDataProductButton" disabled={!selectedDataProduct} onClick={onOk}>
                        Save
                    </Button>
                </>
            }
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
