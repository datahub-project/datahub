import Modal from 'antd/lib/modal/Modal';
import { Button, Select, Spin, message } from 'antd';
import React, { useEffect, useRef, useState } from 'react';
import styled from 'styled-components';
import { useGetAutoCompleteMultipleResultsLazyQuery } from '../../../../../../../graphql/search.generated';
import { DataProduct, Entity, EntityType } from '../../../../../../../types.generated';
import { useEnterKeyListener } from '../../../../../../shared/useEnterKeyListener';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { IconStyleType } from '../../../../../Entity';
import { tagRender } from '../tagRenderer';
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
    const [searchResults, setSearchResults] = useState<Entity[]>([]);


    const [getSearchResults, { data, loading }] = useGetAutoCompleteMultipleResultsLazyQuery();

    const handleSearch = (text: string) => {
        if (text) {
        getSearchResults({
            variables: {
                input: {
                    types: [EntityType.DataProduct],
                    query: text,
                    limit: 5,
                },
            },
        });
        }
    };


    useEffect(() => {
        const newData: Array<Entity> =
            data?.autoCompleteForMultiple?.suggestions.flatMap((suggestion) => suggestion.entities) || [];
        setSearchResults(newData);
    }, [data]);

    function onOk() {
        if (!selectedDataProduct) return;

        if (onOkOverride) {
            onOkOverride(selectedDataProduct?.urn);
            return;
        }

        batchSetDataProductMutation({
            variables: { input: { resourceUrns: urns, dataProductUrn: selectedDataProduct.urn } },
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
        const dataProduct = data?.autoCompleteForMultiple?.suggestions
            .flatMap((suggestion) => suggestion.entities || [])
            .find((entity) => entity.urn === urn);
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

    return (
        <Modal
            title={titleOverride || 'Set Data Product'}
            open
            onCancel={onModalClose}
            footer={
                <>
                    <Button onClick={onModalClose} type="text">
                        Cancel
                    </Button>
                    <Button id="setDataProductButton" disabled={!selectedDataProduct} onClick={onOk}>
                        Add
                    </Button>
                </>
            }
        >
            <Select
                autoFocus
                defaultOpen
                filterOption={false}
                showSearch
                mode="multiple"
                defaultActiveFirstOption={false}
                placeholder="Search for Data Products..."
                onSelect={(urn: string) => onSelectDataProduct(urn)}
                onDeselect={onDeselect}
                onSearch={(value: string) => {
                    handleSearch(value.trim());
                }}
                style={{ width: '100%' }}
                ref={inputEl}
                value={selectValue}
                tagRender={tagRender}
            >
                {loading && (
                    <Select.Option>
                        <LoadingWrapper>
                            <Spin size="default" />
                        </LoadingWrapper>
                    </Select.Option>
                )}
                {searchResults.map((result) => (
                    <Select.Option value={result.urn} key={result.urn}>
                        <OptionWrapper>
                            {entityRegistry.getIcon(EntityType.DataProduct, 12, IconStyleType.ACCENT, 'black')}
                            {entityRegistry.getDisplayName(EntityType.DataProduct, result)}
                        </OptionWrapper>
                    </Select.Option>
                ))}
            </Select>
        </Modal>
    );
}
