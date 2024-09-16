import React, { useState } from 'react';
import styled from 'styled-components';

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
    const { fieldTypes } = props;

    // Ensure state properties are always arrays or boolean
    const { terms = [], nodes = [], tags = [], termsEnabled = false, tagsEnabled = false } = state;

    const getRadioValue = (enabled: boolean, itemCount: number) => {
        if (enabled && itemCount > 0) return 'some' as RadioValue;
        if (enabled) return 'all' as RadioValue;
        return 'none' as RadioValue;
    };

    const [selected, setSelected] = useState({
        terms: {
            selectionType: getRadioValue(termsEnabled, terms.length + nodes.length),
            selected: {
                [EntityType.GlossaryTerm]: terms,
                [EntityType.GlossaryNode]: nodes,
            },
        },
        tags: {
            selectionType: getRadioValue(tagsEnabled, tags.length),
            selected: {
                [EntityType.Tag]: tags,
            },
        },
    });

    const handleTermsChange = (values: any, entity: EntityType, type: string) => {
        const newTerms = {
            ...selected[type],
            selectionType: values.selectionType,
            selected: {
                ...selected[type].selected,
                [entity]: values.selected[entity],
            },
        };

        if ((type === 'terms' && values.selectionType === 'all') || values.selectionType === 'none') {
            newTerms.selected[EntityType.GlossaryNode] = [];
            newTerms.selected[EntityType.GlossaryTerm] = [];
        }

        if ((type === 'tags' && values.selectionType === 'all') || values.selectionType === 'none') {
            newTerms.selected[EntityType.Tag] = [];
        }

        const newData = { ...selected, [type]: newTerms };

        setSelected(newData);

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
                                    preselectedOptions: selected.terms.selected[EntityType.GlossaryTerm],
                                    enabled: fieldTypes.includes(EntityType.GlossaryTerm),
                                },
                                {
                                    label: 'Term Groups',
                                    type: EntityType.GlossaryNode,
                                    preselectedOptions: selected.terms.selected[EntityType.GlossaryNode],
                                    enabled: fieldTypes.includes(EntityType.GlossaryNode),
                                },
                            ]}
                            radio={{
                                preselectedValue: selected.terms.selectionType,
                            }}
                            onChange={(values, entity) => handleTermsChange(values, entity, 'terms')}
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
                                    preselectedOptions: selected.tags.selected[EntityType.Tag],
                                    enabled: fieldTypes.includes(EntityType.Tag),
                                },
                            ]}
                            radio={{
                                preselectedValue: selected.tags.selectionType,
                            }}
                            onChange={(values, entity) => handleTermsChange(values, entity, 'tags')}
                        />
                    );
                }

                return null;
            })}
        </Wrapper>
    );
};
