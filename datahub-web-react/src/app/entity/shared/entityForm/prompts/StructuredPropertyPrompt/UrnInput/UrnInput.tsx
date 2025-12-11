/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { LoadingOutlined } from '@ant-design/icons';
import { Select } from 'antd';
import React from 'react';
import styled from 'styled-components';

import SelectedEntity from '@app/entity/shared/entityForm/prompts/StructuredPropertyPrompt/UrnInput/SelectedEntity';
import useUrnInput from '@app/entity/shared/entityForm/prompts/StructuredPropertyPrompt/UrnInput/useUrnInput';

import { StructuredPropertyEntity } from '@types';

const EntitySelect = styled(Select)`
    width: 75%;
    min-width: 400px;
    max-width: 600px;

    .ant-select-selector {
        padding: 4px;
    }
`;

const LoadingWrapper = styled.div`
    padding: 8px;
    display: flex;
    justify-content: center;

    svg {
        height: 24px;
        width: 24px;
    }
`;

interface Props {
    structuredProperty: StructuredPropertyEntity;
    selectedValues: any[];
    updateSelectedValues: (values: string[] | number[]) => void;
}

export default function UrnInput({ structuredProperty, selectedValues, updateSelectedValues }: Props) {
    const {
        onSelectValue,
        onDeselectValue,
        handleSearch,
        tagRender,
        selectedEntities,
        searchResults,
        loading,
        entityTypeNames,
    } = useUrnInput({ structuredProperty, selectedValues, updateSelectedValues });

    const placeholder = `Search for ${entityTypeNames ? entityTypeNames.map((name) => ` ${name}`) : 'entities'}...`;

    return (
        <EntitySelect
            mode="multiple"
            filterOption={false}
            placeholder={placeholder}
            showSearch
            defaultActiveFirstOption={false}
            onSelect={(urn: any) => onSelectValue(urn)}
            onDeselect={(urn: any) => onDeselectValue(urn)}
            onSearch={(value: string) => handleSearch(value.trim())}
            tagRender={tagRender}
            value={selectedEntities.map((e) => e.urn)}
            loading={loading}
            notFoundContent={
                loading ? (
                    <LoadingWrapper>
                        <LoadingOutlined />
                    </LoadingWrapper>
                ) : undefined
            }
        >
            {searchResults?.map((searchResult) => (
                <Select.Option value={searchResult.urn} key={searchResult.urn}>
                    <SelectedEntity entity={searchResult} />
                </Select.Option>
            ))}
        </EntitySelect>
    );
}
