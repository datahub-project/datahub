import { LoadingOutlined } from '@ant-design/icons';
import { Select } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { Entity, EntityType } from '../../../../../../../types.generated';
import useUrnInput from './useUrnInput';
import SelectedEntity from './SelectedEntity';

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
    selectedValues: any[];
    updateSelectedValues: (values: any[]) => void;
    initialEntities: Entity[];
    allowedEntityTypes?: EntityType[];
    isMultiple: boolean;
}

export default function UrnInput({
    initialEntities,
    allowedEntityTypes,
    isMultiple,
    selectedValues,
    updateSelectedValues,
}: Props) {
    const {
        onSelectValue,
        onDeselectValue,
        handleSearch,
        tagRender,
        selectedEntities,
        searchResults,
        loading,
        entityTypeNames,
    } = useUrnInput({ initialEntities, allowedEntityTypes, isMultiple, selectedValues, updateSelectedValues });

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
