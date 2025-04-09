import { LoadingOutlined } from '@ant-design/icons';
import { Select } from 'antd';
import React, { useRef } from 'react';
import GlossaryBrowser from '@src/app/glossary/GlossaryBrowser/GlossaryBrowser';
import ClickOutside from '@src/app/shared/ClickOutside';
import { BrowserWrapper } from '@src/app/shared/tags/AddTagsTermsModal';
import styled from 'styled-components';
import { Entity, EntityType, FormPromptType } from '../../../../../../../types.generated';
import SelectedEntity from './SelectedEntity';
import useUrnInput from './useUrnInput';

const EntitySelect = styled(Select)`
    width: 100%;
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
    allowedEntities?: Entity[];
    allowedEntityTypes?: EntityType[];
    isMultiple: boolean;
    promptType?: FormPromptType;
    placeholder?: string;
}

const GLOSSARY_TERM_PROMPT_TYPES = [FormPromptType.GlossaryTerms, FormPromptType.FieldsGlossaryTerms];

export default function UrnInput({
    initialEntities,
    allowedEntities,
    allowedEntityTypes,
    isMultiple,
    selectedValues,
    updateSelectedValues,
    promptType,
    placeholder,
}: Props) {
    const {
        onSelectValue,
        onDeselectValue,
        onSelectEntity,
        handleSearch,
        tagRender,
        selectedEntities,
        entityOptions,
        loading,
        entityTypeNames,
        searchValue,
        setSearchValue,
        isFocused,
        setIsFocused,
    } = useUrnInput({
        initialEntities,
        allowedEntities,
        allowedEntityTypes,
        isMultiple,
        selectedValues,
        updateSelectedValues,
    });
    const inputEl = useRef(null);

    const displayedPlaceholder =
        placeholder || `Search for ${entityTypeNames ? entityTypeNames.map((name) => ` ${name}`) : 'assets'}...`;
    const canShowGlossaryBrowser =
        promptType && GLOSSARY_TERM_PROMPT_TYPES.includes(promptType) && !allowedEntities?.length;
    const isShowingGlossaryBrowser = canShowGlossaryBrowser && !searchValue;

    return (
        <ClickOutside
            onClickOutside={() => setIsFocused(false)}
            style={{ width: '75%', minWidth: 400, maxWidth: 600, position: 'relative' }}
        >
            <EntitySelect
                mode="multiple"
                filterOption={false}
                placeholder={displayedPlaceholder}
                showSearch
                ref={inputEl}
                defaultActiveFirstOption={false}
                onSelect={(urn: any) => {
                    onSelectValue(urn);
                    setSearchValue('');
                    if (canShowGlossaryBrowser && inputEl && inputEl.current) {
                        (inputEl.current as any).blur();
                    }
                }}
                onDeselect={(urn: any) => {
                    onDeselectValue(urn);
                    setSearchValue('');
                }}
                onSearch={(value: string) => handleSearch(value)}
                tagRender={tagRender}
                value={selectedEntities.map((e) => e.urn)}
                searchValue={searchValue}
                onFocus={() => setIsFocused(true)}
                onBlur={() => setSearchValue('')}
                loading={loading}
                notFoundContent={
                    loading ? (
                        <LoadingWrapper>
                            <LoadingOutlined />
                        </LoadingWrapper>
                    ) : undefined
                }
                dropdownStyle={isShowingGlossaryBrowser ? { display: 'none' } : {}}
            >
                {entityOptions?.map((entity) => (
                    <Select.Option value={entity.urn} key={entity.urn}>
                        <SelectedEntity entity={entity} />
                    </Select.Option>
                ))}
            </EntitySelect>
            <BrowserWrapper
                isHidden={!isShowingGlossaryBrowser || !isFocused}
                width="100%"
                minWidth={400}
                maxWidth={600}
            >
                <GlossaryBrowser
                    isSelecting
                    selectTerm={(urn, displayName) => {
                        onSelectEntity({
                            urn,
                            type: EntityType.GlossaryTerm,
                            properties: { name: displayName },
                        } as Entity);
                        setIsFocused(false);
                    }}
                />
            </BrowserWrapper>
        </ClickOutside>
    );
}
