/*
 * Resuable Term Selector Component
 * Please keep this agnostic and reusable
 */

import React, { useEffect, useRef } from 'react';

import styled from 'styled-components';

import { EntityType } from '@src/types.generated';
import { TagsAndTermsSelected } from '@app/automations/types';

import { TermOption } from './TermOption';
import type { RadioValue } from './types';

const Wrapper = styled.div`
    display: grid;
    gap: 16px;
`;

interface Props {
    termsSelected: TagsAndTermsSelected;
    termPropagationEnabled: boolean;
    tagPropagationEnabled: boolean;
    setTermsSelected: (terms: TagsAndTermsSelected) => void;
    setTermPropagationEnabled: (enabled: boolean) => void;
    setTagPropagationEnabled: (enabled: boolean) => void;
    fieldTypes: EntityType[];
}

// Component
export const TermSelector = ({
    termsSelected,
    termPropagationEnabled,
    tagPropagationEnabled,
    setTermsSelected,
    setTermPropagationEnabled,
    setTagPropagationEnabled,
    fieldTypes,
}: Props) => {
    // Get the radio value based on the enabled state
    const getRadioValue = (enabled: boolean) => {
        if (!enabled) {
            return 'none' as RadioValue;
        }
        return 'some' as RadioValue;
    };

    // State
    const [optionSelected, setOptionsSelected] = React.useState({
        terms: {
            selectionType: getRadioValue(termPropagationEnabled),
            selected: {
                [EntityType.GlossaryTerm]: termsSelected.terms || [],
                [EntityType.GlossaryNode]: termsSelected.nodes || [],
            },
        },
        tags: {
            selectionType: getRadioValue(tagPropagationEnabled),
            selected: {
                [EntityType.Tag]: termsSelected.tags || [],
            },
        },
    });

    const prevProps = useRef(optionSelected);

    // Handle the change of the selected terms
    const handleTermsChange = (values: any, type: string) => {
        const newTerms = {
            ...optionSelected[type],
            selectionType: values.selectionType,
            selected: values.selected,
        };

        if (type === 'terms' && values.selectionType === 'all') {
            newTerms.selected[EntityType.GlossaryNode] = [];
            newTerms.selected[EntityType.GlossaryTerm] = [];
        }

        if (type === 'tags' && values.selectionType === 'all') {
            newTerms.selected[EntityType.Tag] = [];
        }

        setOptionsSelected({ ...optionSelected, [type]: newTerms });
    };

    // Send the data back to the parent component
    // Only sends the data if the form data has changed
    useEffect(() => {
        const prevData = prevProps.current;
        const hasChanged = JSON.stringify(prevData) !== JSON.stringify(optionSelected);
        if (hasChanged) {
            setTermsSelected({
                terms:
                    optionSelected.terms.selectionType === 'some'
                        ? optionSelected.terms.selected[EntityType.GlossaryTerm]
                        : [],
                nodes:
                    optionSelected.terms.selectionType === 'some'
                        ? optionSelected.terms.selected[EntityType.GlossaryNode]
                        : [],
                tags: optionSelected.tags.selectionType === 'some' ? optionSelected.tags.selected[EntityType.Tag] : [],
            });

            setTermPropagationEnabled(optionSelected.terms.selectionType !== 'none');
            setTagPropagationEnabled(optionSelected.tags.selectionType !== 'none');
        }
        prevProps.current = optionSelected;
    }, [optionSelected, setTermsSelected, setTermPropagationEnabled, setTagPropagationEnabled]);

    return (
        <Wrapper>
            {fieldTypes.map((fieldType) => {
                if (fieldType === EntityType.GlossaryTerm) {
                    return (
                        <TermOption
                            shortType="terms"
                            selects={[
                                {
                                    label: 'Glossary Terms',
                                    type: EntityType.GlossaryTerm,
                                    preselectedOptions: optionSelected.terms.selected[EntityType.GlossaryTerm],
                                    enabled: fieldTypes.includes(EntityType.GlossaryTerm),
                                },
                                {
                                    label: 'Term Groups',
                                    type: EntityType.GlossaryNode,
                                    preselectedOptions: optionSelected.terms.selected[EntityType.GlossaryNode],
                                    enabled: fieldTypes.includes(EntityType.GlossaryNode),
                                },
                            ]}
                            radio={{
                                preselectedValue: optionSelected.terms.selectionType,
                            }}
                            onChange={(values) => handleTermsChange(values, 'terms')}
                        />
                    );
                }
                if (fieldType === EntityType.Tag) {
                    return (
                        <TermOption
                            shortType="tags"
                            selects={[
                                {
                                    label: 'Tags',
                                    type: EntityType.Tag,
                                    preselectedOptions: optionSelected.tags.selected[EntityType.Tag],
                                    enabled: fieldTypes.includes(EntityType.Tag),
                                },
                            ]}
                            radio={{
                                preselectedValue: optionSelected.tags.selectionType,
                            }}
                            onChange={(values) => handleTermsChange(values, 'tags')}
                        />
                    );
                }

                return null;
            })}
        </Wrapper>
    );
};
