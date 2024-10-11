import React, { useEffect, useState, useCallback } from 'react';
import styled from 'styled-components';
import { isEqual } from 'lodash';

import { EntityType } from '@src/types.generated';
import type { ComponentBaseProps } from '@app/automations/types';
import type { RadioValue } from './types';

import { TermOption } from './TermOption';

const Wrapper = styled.div`
    display: grid;
    gap: 16px;
`;

export type TermSelectorStateType = {
    terms?: string[];
    nodes?: string[];
    tags?: string[];
    termsEnabled?: boolean;
    tagsEnabled?: boolean;
};

export const TermSelector = ({ state, props, passStateToParent }: ComponentBaseProps) => {
    const { fieldTypes, allowedRadios } = props;

    // Ensure state properties are always arrays or boolean
    const { terms = [], nodes = [], tags = [], termsEnabled = false, tagsEnabled = false } = state;

    const getRadioValue = useCallback(
        (enabled: boolean) => {
            if (allowedRadios.length === 1) {
                return allowedRadios[0]; // If there's only one radio, return it.
            }

            if (enabled) {
                if (state?.terms?.length > 0 || state?.nodes?.length > 0) return 'some' as RadioValue;
                if (state?.tags?.length > 0) return 'some' as RadioValue;
                if (state?.tags?.length === 0 || state?.nodes?.length === 0 || state?.terms?.length === 0)
                    return 'all' as RadioValue;
            }

            if (!enabled) return 'none' as RadioValue;

            // Default to all
            return 'all' as RadioValue;
        },
        [allowedRadios, state],
    );

    const [selected, setSelected] = useState({
        terms: {
            selectionType: getRadioValue(termsEnabled),
            selected: {
                [EntityType.GlossaryTerm]: terms,
                [EntityType.GlossaryNode]: nodes,
            },
        },
        tags: {
            selectionType: getRadioValue(tagsEnabled),
            selected: {
                [EntityType.Tag]: tags,
            },
        },
    });

    useEffect(() => {
        const newState = {
            terms: {
                selectionType: getRadioValue(termsEnabled),
                selected: {
                    [EntityType.GlossaryTerm]: terms,
                    [EntityType.GlossaryNode]: nodes,
                },
            },
            tags: {
                selectionType: getRadioValue(tagsEnabled),
                selected: {
                    [EntityType.Tag]: tags,
                },
            },
        };

        if (!isEqual(selected, newState)) {
            setSelected(newState);
        }
    }, [terms, nodes, tags, termsEnabled, tagsEnabled, selected, getRadioValue]);

    const handleTermsChange = (values: any, entities: EntityType[], type: string) => {
        const newTerms = {
            ...selected[type],
            selectionType: values.selectionType,
            selected: entities.reduce(
                (acc, entity) => ({
                    ...acc,
                    [entity]: values.selected[entity],
                }),
                { ...selected[type].selected },
            ),
        };

        if (type === 'terms' && values.selectionType === 'all') {
            newTerms.selected[EntityType.GlossaryNode] = [];
            newTerms.selected[EntityType.GlossaryTerm] = [];
        }

        if (type === 'tags' && values.selectionType === 'all') {
            newTerms.selected[EntityType.Tag] = [];
        }

        const newData = { ...selected, [type]: newTerms };

        passStateToParent({
            termsEnabled: newData.terms.selectionType !== 'none',
            tagsEnabled: newData.tags.selectionType !== 'none',
            terms: newData.terms.selectionType === 'some' ? newData.terms.selected[EntityType.GlossaryTerm] : [],
            nodes: newData.terms.selectionType === 'some' ? newData.terms.selected[EntityType.GlossaryNode] : [],
            tags: newData.tags.selectionType === 'some' ? newData.tags.selected[EntityType.Tag] : [],
        });
    };

    return (
        <Wrapper>
            {fieldTypes.map((fieldType) => {
                if (fieldType === EntityType.GlossaryTerm) {
                    return (
                        <TermOption
                            key="terms"
                            shortType="terms"
                            selects={[
                                {
                                    label: 'Glossary Terms',
                                    type: EntityType.GlossaryTerm,
                                    preselectedOptions: terms || [],
                                    enabled: fieldTypes.includes(EntityType.GlossaryTerm),
                                },
                                {
                                    label: 'Term Groups',
                                    type: EntityType.GlossaryNode,
                                    preselectedOptions: nodes || [],
                                    enabled: fieldTypes.includes(EntityType.GlossaryNode),
                                },
                            ]}
                            radio={{
                                allowedRadios,
                                preselectedValue: selected.terms.selectionType,
                            }}
                            onChange={(values) => {
                                handleTermsChange(values, [EntityType.GlossaryNode, EntityType.GlossaryTerm], 'terms');
                            }}
                        />
                    );
                }
                if (fieldType === EntityType.Tag) {
                    return (
                        <TermOption
                            key="tags"
                            shortType="tags"
                            selects={[
                                {
                                    label: 'Tags',
                                    type: EntityType.Tag,
                                    preselectedOptions: tags || [],
                                    enabled: fieldTypes.includes(EntityType.Tag),
                                },
                            ]}
                            radio={{
                                allowedRadios,
                                preselectedValue: selected.tags.selectionType,
                            }}
                            onChange={(values, entity) => handleTermsChange(values, [entity as EntityType], 'tags')}
                        />
                    );
                }

                return null;
            })}
        </Wrapper>
    );
};
