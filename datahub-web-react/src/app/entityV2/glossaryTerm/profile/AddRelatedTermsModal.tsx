import { Modal, SimpleSelect, toast } from '@components';
import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { SelectOption } from '@components/components/Select/types';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import GlossaryBrowser from '@app/glossary/GlossaryBrowser/GlossaryBrowser';
import GlossaryTermPill from '@app/glossaryV2/GlossaryTermPill';
import { useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import ParentEntities from '@app/searchV2/filters/ParentEntities';
import { getParentEntities } from '@app/searchV2/filters/utils';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useAddRelatedTermsMutation } from '@graphql/glossaryTerm.generated';
import { useGetSearchResultsLazyQuery } from '@graphql/search.generated';
import { DataHubPageModuleType, EntityType, SearchResult, TermRelationshipType } from '@types';

const SearchResultContainer = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: center;
    font-size: 12px;
`;

const BrowserEmptyState = styled.div`
    max-height: 320px;
    overflow: auto;
`;

interface Props {
    onClose: () => void;
    relationshipType: TermRelationshipType;
}

interface TermOption extends SelectOption {
    entity?: SearchResult['entity'];
}

function AddRelatedTermsModal(props: Props) {
    const { onClose, relationshipType } = props;

    const { t } = useTranslation('entity.types');
    const { t: tc } = useTranslation('common.actions');
    const [inputValue, setInputValue] = useState('');
    const [selectedUrns, setSelectedUrns] = useState<string[]>([]);
    const [selectedTerms, setSelectedTerms] = useState<{ urn: string; displayName: string }[]>([]);
    const entityRegistry = useEntityRegistry();
    const { urn: entityDataUrn } = useEntityData();
    const refetch = useRefetch();
    const { reloadByKeyType } = useReloadableContext();
    const generateTermColor = useGenerateGlossaryColorFromPalette();

    const [AddRelatedTerms] = useAddRelatedTermsMutation();

    function addTerms() {
        AddRelatedTerms({
            variables: {
                input: {
                    urn: entityDataUrn,
                    termUrns: selectedUrns,
                    relationshipType,
                },
            },
        })
            .catch((e) => {
                toast.destroy();
                toast.error(t('glossaryTerm.moveError', { error: e.message || '' }), { duration: 3 });
            })
            .finally(() => {
                toast.loading(t('glossaryTerm.adding'), { duration: 2 });
                setTimeout(() => {
                    toast.success(t('glossaryTerm.addedRelatedTermsSuccess'), { duration: 2 });
                    refetch();
                    // Reload modules
                    // RelatedTerms - update related terms module on term summary tab
                    reloadByKeyType([
                        getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.RelatedTerms),
                    ]);
                }, 2000);
            });
        onClose();
    }

    const [termSearch, { data: termSearchData }] = useGetSearchResultsLazyQuery();
    const termSearchResults = termSearchData?.search?.searchResults || [];

    const handleSearch = (text: string) => {
        const trimmed = text.trim();
        setInputValue(trimmed);
        if (trimmed.length > 0) {
            termSearch({
                variables: {
                    input: {
                        type: EntityType.GlossaryTerm,
                        query: trimmed,
                        start: 0,
                        count: 20,
                    },
                },
            });
        }
    };

    const options: TermOption[] = useMemo(
        () =>
            termSearchResults
                .filter((result) => result?.entity?.urn !== entityDataUrn)
                .map((result: SearchResult) => ({
                    value: result.entity.urn,
                    label: entityRegistry.getDisplayName(result.entity.type, result.entity),
                    entity: result.entity,
                })),
        [termSearchResults, entityDataUrn, entityRegistry],
    );

    const combinedOptions: TermOption[] = useMemo(() => {
        const byUrn = new Map(options.map((option) => [option.value, option]));
        selectedTerms.forEach((term) => {
            if (!byUrn.has(term.urn)) {
                byUrn.set(term.urn, { value: term.urn, label: term.displayName });
            }
        });
        return Array.from(byUrn.values());
    }, [options, selectedTerms]);

    const onUpdate = useCallback(
        (urns: string[]) => {
            setSelectedUrns(urns);
            setSelectedTerms((prev) => {
                const prevByUrn = new Map(prev.map((term) => [term.urn, term]));
                return urns.map((urn) => {
                    const existing = prevByUrn.get(urn);
                    if (existing) return existing;
                    const fromOptions = combinedOptions.find((option) => option.value === urn);
                    return {
                        urn,
                        displayName: fromOptions?.label || urn,
                    };
                });
            });
        },
        [combinedOptions],
    );

    function selectTermFromBrowser(urn: string, displayName: string) {
        if (selectedUrns.includes(urn)) return;
        setSelectedUrns((prev) => [...prev, urn]);
        setSelectedTerms((prev) => [...prev, { urn, displayName }]);
    }

    const renderOption = useCallback(
        (option: TermOption) => (
            <SearchResultContainer>
                {option.entity && <ParentEntities parentEntities={getParentEntities(option.entity) || []} />}
                <GlossaryTermPill
                    name={option.label}
                    color={generateTermColor(option.value)}
                    variant="borderless"
                />
            </SearchResultContainer>
        ),
        [generateTermColor],
    );

    const renderSelectedValue = useCallback(
        (option: TermOption) => (
            <GlossaryTermPill
                key={option.value}
                name={option.label}
                color={generateTermColor(option.value)}
                variant="borderless"
            />
        ),
        [generateTermColor],
    );

    const isShowingGlossaryBrowser = !inputValue;

    return (
        <Modal
            title={t('glossaryTerm.addRelatedTermsTitle')}
            open
            onCancel={onClose}
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onClose,
                },
                {
                    text: tc('add'),
                    onClick: addTerms,
                    variant: 'filled',
                    disabled: !selectedUrns.length,
                    buttonDataTestId: 'submit-button',
                },
            ]}
        >
            <SimpleSelect
                showSearch
                isMultiSelect
                filterResultsByQuery={false}
                placeholder={t('glossaryTerm.searchForTermsPlaceholder')}
                values={selectedUrns}
                onUpdate={onUpdate}
                onSearchChange={handleSearch}
                onClear={() => {
                    setInputValue('');
                    setSelectedUrns([]);
                    setSelectedTerms([]);
                }}
                options={isShowingGlossaryBrowser ? [] : options}
                combinedSelectedAndSearchOptions={combinedOptions}
                width="full"
                showClear
                ignoreMaxHeight={isShowingGlossaryBrowser}
                dataTestId="related-terms-select"
                selectLabelProps={{ variant: 'custom' }}
                renderCustomOptionText={renderOption}
                renderCustomSelectedValue={renderSelectedValue}
                emptyState={
                    isShowingGlossaryBrowser ? (
                        <BrowserEmptyState>
                            <GlossaryBrowser
                                isSelecting
                                selectTerm={selectTermFromBrowser}
                                termUrnToHide={entityDataUrn}
                            />
                        </BrowserEmptyState>
                    ) : undefined
                }
            />
        </Modal>
    );
}

export default AddRelatedTermsModal;
