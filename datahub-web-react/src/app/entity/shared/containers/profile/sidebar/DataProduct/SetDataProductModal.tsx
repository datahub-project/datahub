import Modal from 'antd/lib/modal/Modal';
import { Button, Select, message } from 'antd';
import React, { useRef, useState } from 'react';
import styled from 'styled-components';
import { useGetSearchResultsForMultipleQuery } from '../../../../../../../graphql/search.generated';
import { DataProduct, EntityType } from '../../../../../../../types.generated';
import { useEnterKeyListener } from '../../../../../../shared/useEnterKeyListener';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { IconStyleType } from '../../../../../Entity';
import { tagRender } from '../tagRenderer';
import { useBatchSetDataProductMutation } from '../../../../../../../graphql/dataProduct.generated';
import { handleBatchError } from '../../../../utils';
import { getModalDomContainer } from '../../../../../../../utils/focus';

const OptionWrapper = styled.div`
    padding: 2px 0;

    svg {
        margin-right: 8px;
    }
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
    const [query, setQuery] = useState<string | undefined>('');
    const inputEl = useRef(null);

    const { data } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [EntityType.DataProduct],
                query: query || '',
                start: 0,
                count: 5,
            },
        },
    });

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
        const dataProduct = data?.searchAcrossEntities?.searchResults
            .map((result) => result.entity)
            .find((entity) => entity.urn === urn);
        setSelectedDataProduct((dataProduct as DataProduct) || null);
    }

    function onDeselect() {
        setQuery('');
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
            getContainer={getModalDomContainer}
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
                onSearch={(value: string) => setQuery(value.trim())}
                style={{ width: '100%' }}
                ref={inputEl}
                value={selectValue}
                tagRender={tagRender}
                onBlur={() => setQuery('')}
            >
                {data?.searchAcrossEntities?.searchResults?.map((result) => (
                    <Select.Option value={result.entity.urn} key={result.entity.urn}>
                        <OptionWrapper>
                            {entityRegistry.getIcon(EntityType.DataProduct, 12, IconStyleType.ACCENT, 'black')}
                            {entityRegistry.getDisplayName(EntityType.DataProduct, result.entity)}
                        </OptionWrapper>
                    </Select.Option>
                ))}
            </Select>
        </Modal>
    );
}
