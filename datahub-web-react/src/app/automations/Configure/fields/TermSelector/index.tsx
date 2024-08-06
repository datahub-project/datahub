/*
 * Resuable Term Selector Component
 * Please keep this agnostic and reusable
 */

import React, { useEffect, useRef, useMemo, useState } from 'react';

import { Select } from 'antd';
import styled from 'styled-components';

import { useGetSearchResultsQuery } from '@graphql/search.generated';
import { EntityType, GlossaryTerm, Tag as TagType, GlossaryNode } from '@src/types.generated';
import { TagTermLabel } from '@app/shared/tags/TagTermLabel';

import { TagsAndTermsSelected } from '@app/automations/types';

const Wrapper = styled.div`
    display: grid;
    grid-template-columns: 1fr 1fr 1fr;
    gap: 8px;
`;

const Label = styled.div`
    font-size: 12px;
    margin-bottom: 2px;
`;

// Clean data
const cleanData = (data: any) =>
    data?.search?.searchResults?.map((searchResult: any) => searchResult.entity as any) || [];

interface Props {
    tagsAndTermsSelected: TagsAndTermsSelected;
    setTagsAndTermsSelected: (terms: TagsAndTermsSelected) => void;
    fieldTypes: EntityType[];
}

// Component
export const TermSelector = ({
    tagsAndTermsSelected: termsSelected,
    setTagsAndTermsSelected: setTermsSelected,
    fieldTypes,
}: Props) => {
    const [selectedTerms, setSelectedTerms] = React.useState<string[]>(termsSelected.terms);
    const [selectedTags, setSelectedTags] = React.useState<string[]>(termsSelected.tags);
    const [selectedNodes, setSelectedNodes] = React.useState<string[]>(termsSelected.nodes);
    const [termsSearchQuery, setTermsSearchQuery] = useState('');
    const [nodesSearchQuery, setNodesSearchQuery] = useState('');
    const [tagsSearchQuery, setTagsSearchQuery] = useState('');

    const prevProps = useRef(termsSelected);

    // Get glossary terms
    const {
        data: glossaryTermData,
        loading: termLoading,
        error: termError,
    } = useGetSearchResultsQuery({
        variables: {
            input: {
                query: termsSearchQuery,
                type: EntityType.GlossaryTerm,
                filters: [],
                start: 0,
                count: 40,
            },
        },
    });

    // Get Tags
    const {
        data: tagData,
        loading: tagLoading,
        error: tagError,
    } = useGetSearchResultsQuery({
        variables: {
            input: {
                query: tagsSearchQuery,
                type: EntityType.Tag,
                filters: [],
                start: 0,
                count: 40,
            },
        },
    });

    // Get glossary nodes
    const {
        data: glossaryNodeData,
        loading: nodeLoading,
        error: nodeError,
    } = useGetSearchResultsQuery({
        variables: {
            input: {
                query: nodesSearchQuery,
                type: EntityType.GlossaryNode,
                filters: [],
                start: 0,
                count: 40,
            },
        },
    });

    // Data
    const terms = cleanData(glossaryTermData);
    const nodes = cleanData(glossaryNodeData);
    const tags = cleanData(tagData);

    // Send the data back to the parent component
    // Only sends the data if the form data has changed
    const combinedState = useMemo(
        () => ({
            terms: selectedTerms,
            nodes: selectedNodes,
            tags: selectedTags,
        }),
        [selectedTerms, selectedNodes, selectedTags],
    );
    useEffect(() => {
        const prevData = prevProps.current;
        const hasChanged = JSON.stringify(prevData) !== JSON.stringify(combinedState);
        if (hasChanged) setTermsSelected(combinedState);
        prevProps.current = combinedState;
    }, [combinedState, setTermsSelected]);

    return (
        <Wrapper>
            {fieldTypes.map((fieldType) => {
                if (fieldType === EntityType.GlossaryTerm) {
                    return (
                        <div>
                            <Label>Glossary Terms</Label>
                            <Select
                                mode="multiple"
                                options={terms.map((term: GlossaryTerm) => ({
                                    label: <TagTermLabel entity={term} termName={term.properties?.name} />,
                                    value: term.urn,
                                }))}
                                loading={termLoading}
                                status={termError && 'error'}
                                value={selectedTerms}
                                onChange={(value) => setSelectedTerms(value)}
                                placeholder="Select Glossary Terms…"
                                allowClear
                                showArrow
                                showSearch
                                onSearch={(q) => setTermsSearchQuery(q)}
                            />
                        </div>
                    );
                }
                if (fieldType === EntityType.GlossaryNode) {
                    return (
                        <div>
                            <Label>Term Groups</Label>
                            <Select
                                mode="multiple"
                                options={nodes.map((term: GlossaryNode) => ({
                                    label: <TagTermLabel entity={term} termName={term.properties?.name} />,
                                    value: term.urn,
                                }))}
                                loading={nodeLoading}
                                status={nodeError && 'error'}
                                value={selectedNodes}
                                onChange={(value) => setSelectedNodes(value)}
                                placeholder="Select Glossary Groups…"
                                allowClear
                                showArrow
                                showSearch
                                onSearch={(q) => setNodesSearchQuery(q)}
                            />
                        </div>
                    );
                }
                if (fieldType === EntityType.Tag) {
                    return (
                        <div>
                            <Label>Tags</Label>
                            <Select
                                mode="multiple"
                                options={tags.map((tag: TagType) => ({
                                    label: <TagTermLabel entity={tag} termName={tag.properties?.name} />,
                                    value: tag.urn,
                                }))}
                                loading={tagLoading}
                                status={tagError && 'error'}
                                value={selectedTags}
                                onChange={(value) => setSelectedTags(value)}
                                placeholder="Select Tags…"
                                allowClear
                                showArrow
                                showSearch
                                onSearch={(q) => setTagsSearchQuery(q)}
                            />
                        </div>
                    );
                }

                return null;
            })}
        </Wrapper>
    );
};
