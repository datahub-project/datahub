import { Loader } from '@components';
import { Select } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
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

interface Props {
    structuredProperty: StructuredPropertyEntity;
    selectedValues: any[];
    updateSelectedValues: (values: string[] | number[]) => void;
}

export default function UrnInput({ structuredProperty, selectedValues, updateSelectedValues }: Props) {
    const { t } = useTranslation('entity.form');
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

    const placeholder = entityTypeNames
        ? t('searchForPlaceholder', { entityTypes: entityTypeNames.join(', ') })
        : t('searchForEntitiesPlaceholder');

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
            notFoundContent={loading ? <Loader size="sm" padding={8} /> : undefined}
        >
            {searchResults?.map((searchResult) => (
                <Select.Option value={searchResult.urn} key={searchResult.urn}>
                    <SelectedEntity entity={searchResult} />
                </Select.Option>
            ))}
        </EntitySelect>
    );
}
