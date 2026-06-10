import { Button, Select, message } from 'antd';
import Modal from 'antd/lib/modal/Modal';
import React, { useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { IconStyleType } from '@app/entity/Entity';
import { tagRender } from '@app/entity/shared/containers/profile/sidebar/tagRenderer';
import { handleBatchError } from '@app/entity/shared/utils';
import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { getModalDomContainer } from '@utils/focus';

import { useBatchSetDataProductMutation } from '@graphql/dataProduct.generated';
import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { DataProduct, EntityType } from '@types';

const OptionWrapper = styled.div`
    padding: 2px 0;

    svg {
        margin-right: 8px;
    }
`;

// Color argument passed to entityRegistry.getIcon — a programmatic value, not display copy.
const ICON_COLOR = 'black';

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
    const { t } = useTranslation('entity.shared.containers');
    const { t: tc } = useTranslation('common.actions');
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
                message.success({ content: t('sidebar.dataProduct.updatedSuccess'), duration: 3 });
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
                        content: t('dataProductModal.addError', { message: e.message || '' }),
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
            title={titleOverride || t('sidebar.dataProduct.setModalTitle')}
            open
            onCancel={onModalClose}
            footer={
                <>
                    <Button onClick={onModalClose} type="text">
                        {tc('cancel')}
                    </Button>
                    <Button id="setDataProductButton" disabled={!selectedDataProduct} onClick={onOk}>
                        {tc('add')}
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
                placeholder={t('sidebar.dataProduct.searchPlaceholder')}
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
                            {entityRegistry.getIcon(EntityType.DataProduct, 12, IconStyleType.ACCENT, ICON_COLOR)}
                            {entityRegistry.getDisplayName(EntityType.DataProduct, result.entity)}
                        </OptionWrapper>
                    </Select.Option>
                ))}
            </Select>
        </Modal>
    );
}
