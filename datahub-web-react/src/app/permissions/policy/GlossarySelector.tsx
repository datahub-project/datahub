import { Select, Tag, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';

import GlossaryBrowser from '@app/glossaryV2/GlossaryBrowser/GlossaryBrowser';
import { createCriterionValueWithEntity, getFieldValues, setFieldValues } from '@app/permissions/policy/policyUtils';
import ClickOutside from '@app/shared/ClickOutside';
import { BrowserWrapper } from '@app/shared/tags/AddTagsTermsModal';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleLazyQuery } from '@graphql/search.generated';
import { Entity, EntityType, ResourceFilter } from '@types';

const SearchResultContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 4px;
`;

const StyledBrowserWrapper = styled(BrowserWrapper)`
    bottom: 36px;
`;

type Props = {
    resources: ResourceFilter;
    setResources: (resources: ResourceFilter) => void;
};

export default function GlossarySelector({ resources, setResources }: Props) {
    const entityRegistry = useEntityRegistry();
    const [glossaryInputValue, setGlossaryInputValue] = useState('');
    const [isFocusedOnGlossaryInput, setIsFocusedOnGlossaryInput] = useState(false);

    const [searchGlossaryEntities, { data: glossarySearchData }] = useGetSearchResultsForMultipleLazyQuery();
    const glossarySearchResults = glossarySearchData?.searchAcrossEntities?.searchResults;

    const glossaryEntities = getFieldValues(resources.filter, 'GLOSSARY') || [];
    const glossaryUrnToDisplayName = new Map();
    glossaryEntities.forEach((glossaryEntity) => {
        const displayName = glossaryEntity.entity
            ? entityRegistry.getDisplayName(glossaryEntity.entity.type, glossaryEntity.entity)
            : glossaryEntity.value;
        glossaryUrnToDisplayName[glossaryEntity.value] = displayName;
    });

    const glossarySelectValue = glossaryEntities.map((criterionValue) => criterionValue.value);
    const isShowingGlossaryBrowser = !glossaryInputValue && isFocusedOnGlossaryInput;

    const onSelectGlossaryEntity = (glossaryUrn: string, glossaryEntity?: Entity) => {
        const filter = resources.filter || {
            criteria: [],
        };
        const entity =
            glossaryEntity ||
            glossarySearchResults?.find((result) => result.entity.urn === glossaryUrn)?.entity ||
            null;
        const updatedFilter = setFieldValues(filter, 'GLOSSARY', [
            ...glossaryEntities,
            createCriterionValueWithEntity(glossaryUrn, entity),
        ]);
        setResources({
            ...resources,
            filter: updatedFilter,
        });
    };

    function selectGlossaryTermFromBrowser(urn: string, displayName: string) {
        const entity =
            glossarySearchResults?.find((result) => result.entity.urn === urn)?.entity ||
            ({ urn, type: EntityType.GlossaryTerm, properties: { name: displayName } } as Entity);
        onSelectGlossaryEntity(urn, entity);
        setIsFocusedOnGlossaryInput(false);
    }

    function selectGlossaryNodeFromBrowser(urn: string, displayName: string) {
        const entity =
            glossarySearchResults?.find((result) => result.entity.urn === urn)?.entity ||
            ({ urn, type: EntityType.GlossaryNode, properties: { name: displayName } } as Entity);
        onSelectGlossaryEntity(urn, entity);
        setIsFocusedOnGlossaryInput(false);
    }

    const onDeselectGlossaryEntity = (glossaryUrn: string) => {
        const filter = resources.filter || {
            criteria: [],
        };
        setResources({
            ...resources,
            filter: setFieldValues(
                filter,
                'GLOSSARY',
                glossaryEntities?.filter((criterionValue) => criterionValue.value !== glossaryUrn),
            ),
        });
    };

    const handleGlossarySearch = (text: string) => {
        const trimmedText: string = text.trim();
        setGlossaryInputValue(trimmedText);
        searchGlossaryEntities({
            variables: {
                input: {
                    types: [EntityType.GlossaryTerm, EntityType.GlossaryNode],
                    query: trimmedText.length > 2 ? trimmedText : '*',
                    start: 0,
                    count: 10,
                },
            },
        });
    };

    const renderSearchResult = (result) => {
        return (
            <SearchResultContainer>
                {entityRegistry.getDisplayName(result.entity.type, result.entity)}
            </SearchResultContainer>
        );
    };

    const displayStringWithMaxLength = (displayStr, length) => {
        return displayStr.length > length
            ? `${displayStr.substring(0, Math.min(length, displayStr.length))}...`
            : displayStr;
    };

    function handleBlurGlossary() {
        setGlossaryInputValue('');
    }

    function handleClickOutsideGlossary() {
        setTimeout(() => setIsFocusedOnGlossaryInput(false), 0);
    }

    return (
        <>
            <Typography.Paragraph>
                The policy will apply to resources with the chosen glossary terms or any term under the chosen glossary
                term groups. If <b>none</b> are selected, the policy will not account for glossary terms.
            </Typography.Paragraph>
            <ClickOutside onClickOutside={handleClickOutsideGlossary}>
                <Select
                    showSearch
                    value={glossarySelectValue}
                    mode="multiple"
                    filterOption={false}
                    placeholder="Select glossary terms or term groups to apply to specific resources."
                    onSelect={(value) => onSelectGlossaryEntity(value)}
                    onDeselect={onDeselectGlossaryEntity}
                    onSearch={handleGlossarySearch}
                    onFocus={() => setIsFocusedOnGlossaryInput(true)}
                    onBlur={handleBlurGlossary}
                    tagRender={(tagProps) => (
                        <Tag closable={tagProps.closable} onClose={tagProps.onClose}>
                            {displayStringWithMaxLength(
                                glossaryUrnToDisplayName[tagProps.value.toString()] || tagProps.value.toString(),
                                75,
                            )}
                        </Tag>
                    )}
                    dropdownStyle={isShowingGlossaryBrowser ? { display: 'none' } : {}}
                >
                    {glossarySearchResults?.map((result) => (
                        <Select.Option key={result.entity.urn} value={result.entity.urn}>
                            {renderSearchResult(result)}
                        </Select.Option>
                    ))}
                </Select>
                <StyledBrowserWrapper isHidden={!isShowingGlossaryBrowser} width="100%" maxHeight={350}>
                    <GlossaryBrowser
                        isSelecting
                        selectTerm={selectGlossaryTermFromBrowser}
                        selectNode={selectGlossaryNodeFromBrowser}
                    />
                </StyledBrowserWrapper>
            </ClickOutside>
        </>
    );
}
