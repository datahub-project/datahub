import { message, Button, Modal, Select, Tag } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { useAddRelatedTermsMutation } from '../../../../graphql/glossaryTerm.generated';
import { useGetSearchResultsLazyQuery } from '../../../../graphql/search.generated';
import { EntityType, SearchResult, TermRelationshipType } from '../../../../types.generated';
import GlossaryBrowser from '../../../glossary/GlossaryBrowser/GlossaryBrowser';
import ClickOutside from '../../../shared/ClickOutside';
import { BrowserWrapper } from '../../../shared/tags/AddTagsTermsModal';
import TermLabel from '../../../shared/TermLabel';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { useEntityData, useRefetch } from '../../../entity/shared/EntityContext';
import ParentEntities from '../../../searchV2/filters/ParentEntities';
import { getParentEntities } from '../../../searchV2/filters/utils';

const StyledSelect = styled(Select)`
    width: 480px;
`;

const SearchResultContainer = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: center;
`;

interface Props {
    onClose: () => void;
    relationshipType: TermRelationshipType;
}

function AddRelatedTermsModal(props: Props) {
    const { onClose, relationshipType } = props;

    const [inputValue, setInputValue] = useState('');
    const [selectedUrns, setSelectedUrns] = useState<any[]>([]);
    const [selectedTerms, setSelectedTerms] = useState<any[]>([]);
    const [isFocusedOnInput, setIsFocusedOnInput] = useState(false);
    const entityRegistry = useEntityRegistry();
    const { urn: entityDataUrn } = useEntityData();
    const refetch = useRefetch();

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
                message.destroy();
                message.error({ content: `Failed to move: \n ${e.message || ''}`, duration: 3 });
            })
            .finally(() => {
                message.loading({ content: 'Adding...', duration: 2 });
                setTimeout(() => {
                    message.success({
                        content: 'Added Related Terms!',
                        duration: 2,
                    });
                    refetch();
                }, 2000);
            });
        onClose();
    }

    const [termSearch, { data: termSearchData }] = useGetSearchResultsLazyQuery();
    const termSearchResults = termSearchData?.search?.searchResults || [];

    const tagSearchOptions = termSearchResults
        .filter((result) => result?.entity?.urn !== entityDataUrn)
        .map((result: SearchResult) => {
            const displayName = entityRegistry.getDisplayName(result.entity.type, result.entity);

            return (
                <Select.Option value={result.entity.urn} key={result.entity.urn} name={displayName}>
                    <SearchResultContainer>
                        <ParentEntities parentEntities={getParentEntities(result.entity) || []} />
                        <TermLabel name={displayName} />
                    </SearchResultContainer>
                </Select.Option>
            );
        });

    const handleSearch = (text: string) => {
        if (text.length > 0) {
            termSearch({
                variables: {
                    input: {
                        type: EntityType.GlossaryTerm,
                        query: text,
                        start: 0,
                        count: 20,
                    },
                },
            });
        }
    };

    // When a Tag or term search result is selected, add the urn to the Urns
    const onSelectValue = (urn: string) => {
        const newUrns = [...selectedUrns, urn];
        setSelectedUrns(newUrns);
        const selectedSearchOption = tagSearchOptions.find((option) => option.props.value === urn);
        setSelectedTerms([...selectedTerms, { urn, component: <TermLabel name={selectedSearchOption?.props.name} /> }]);
    };

    // When a Tag or term search result is deselected, remove the urn from the Owners
    const onDeselectValue = (urn: string) => {
        const newUrns = selectedUrns.filter((u) => u !== urn);
        setSelectedUrns(newUrns);
        setInputValue('');
        setIsFocusedOnInput(true);
        setSelectedTerms(selectedTerms.filter((term) => term.urn !== urn));
    };

    function selectTermFromBrowser(urn: string, displayName: string) {
        setIsFocusedOnInput(false);
        const newUrns = [...selectedUrns, urn];
        setSelectedUrns(newUrns);
        setSelectedTerms([...selectedTerms, { urn, component: <TermLabel name={displayName} /> }]);
    }

    function clearInput() {
        setInputValue('');
        setTimeout(() => setIsFocusedOnInput(true), 0); // call after click outside
    }

    function handleBlur() {
        setInputValue('');
    }

    const tagRender = (properties) => {
        // eslint-disable-next-line react/prop-types
        const { closable, onClose: close, value } = properties;
        const onPreventMouseDown = (event) => {
            event.preventDefault();
            event.stopPropagation();
        };
        const selectedItem = selectedTerms.find((term) => term.urn === value).component;

        return (
            <Tag
                onMouseDown={onPreventMouseDown}
                closable={closable}
                onClose={close}
                style={{
                    marginRight: 3,
                    display: 'flex',
                    justifyContent: 'start',
                    alignItems: 'center',
                    whiteSpace: 'nowrap',
                    opacity: 1,
                    color: '#434343',
                    lineHeight: '16px',
                }}
            >
                {selectedItem}
            </Tag>
        );
    };

    const isShowingGlossaryBrowser = !inputValue && isFocusedOnInput;

    return (
        <Modal
            title="Add Related Terms"
            visible
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button type="primary" onClick={addTerms} disabled={!selectedUrns.length}>
                        Add
                    </Button>
                </>
            }
        >
            <ClickOutside onClickOutside={() => setIsFocusedOnInput(false)}>
                <StyledSelect
                    autoFocus
                    mode="multiple"
                    filterOption={false}
                    placeholder="Search for Glossary Terms..."
                    showSearch
                    defaultActiveFirstOption={false}
                    onSelect={(asset: any) => onSelectValue(asset)}
                    onDeselect={(asset: any) => onDeselectValue(asset)}
                    onSearch={(value: string) => {
                        // eslint-disable-next-line react/prop-types
                        handleSearch(value.trim());
                        // eslint-disable-next-line react/prop-types
                        setInputValue(value.trim());
                    }}
                    tagRender={tagRender}
                    value={selectedUrns}
                    onClear={clearInput}
                    onFocus={() => setIsFocusedOnInput(true)}
                    onBlur={handleBlur}
                    dropdownStyle={isShowingGlossaryBrowser || !inputValue ? { display: 'none' } : {}}
                >
                    {tagSearchOptions}
                </StyledSelect>
                <BrowserWrapper isHidden={!isShowingGlossaryBrowser}>
                    <GlossaryBrowser isSelecting selectTerm={selectTermFromBrowser} termUrnToHide={entityDataUrn} />
                </BrowserWrapper>
            </ClickOutside>
        </Modal>
    );
}

export default AddRelatedTermsModal;
