import { Alert } from 'antd';
import { isEqual } from 'lodash';
import React, { useCallback, useEffect, useState } from 'react';
import styled from 'styled-components';

import { TermOption } from '@app/automations/fields/TermSelector/TermOption';
import type { RadioValue } from '@app/automations/fields/TermSelector/types';
import type { ComponentBaseProps } from '@app/automations/types';
import { EntityType } from '@src/types.generated';

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
    const { fieldTypes, allowedRadios, canShowNotice } = props;

    // Ensure state properties are always arrays or boolean
    const { terms = [], nodes = [], tags = [], termsEnabled = false, tagsEnabled = false } = state;

    const getRadioValue = useCallback(
        (enabled: boolean, selectedValues: string[]) => {
            if (allowedRadios.length === 1) {
                return allowedRadios[0]; // If there's only one radio, return it.
            }

            if (enabled && selectedValues?.length) {
                return 'some' as RadioValue;
            }

            if (!enabled) return 'none' as RadioValue;

            // Default to all
            return 'all' as RadioValue;
        },
        [allowedRadios],
    );

    const [selected, setSelected] = useState({
        terms: {
            selectionType: getRadioValue(termsEnabled, [...terms, ...nodes]),
            selected: {
                [EntityType.GlossaryTerm]: terms,
                [EntityType.GlossaryNode]: nodes,
            },
        },
        tags: {
            selectionType: getRadioValue(tagsEnabled, [...tags]),
            selected: {
                [EntityType.Tag]: tags,
            },
        },
    });

    useEffect(() => {
        const newState = {
            terms: {
                selectionType: getRadioValue(termsEnabled, [...terms, ...nodes]),
                selected: {
                    [EntityType.GlossaryTerm]: terms,
                    [EntityType.GlossaryNode]: nodes,
                },
            },
            tags: {
                selectionType: getRadioValue(tagsEnabled, [...tags]),
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

    // Show notice of Ingestion Overwrite if `some` selected
    const showNotice =
        canShowNotice && (selected.tags.selectionType === 'some' || selected.terms.selectionType === 'some');

    return (
        <>
            <Wrapper>
                {fieldTypes.map((fieldType) => {
                    if (fieldType === EntityType.GlossaryTerm) {
                        return (
                            <TermOption
                                key="terms"
                                type={EntityType.GlossaryTerm}
                                typeName="terms"
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
                                onChange={(values, entities: EntityType[]) =>
                                    handleTermsChange(values, entities, 'terms')
                                }
                                isEdit={props.isEdit}
                                shouldUseGlossaryTermComponent={allowedRadios.length === 1}
                            />
                        );
                    }
                    if (fieldType === EntityType.Tag) {
                        return (
                            <TermOption
                                key="tags"
                                type={EntityType.Tag}
                                typeName="tags"
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
                                onChange={(values, entities: EntityType[]) =>
                                    handleTermsChange(values, entities, 'tags')
                                }
                            />
                        );
                    }

                    return null;
                })}
            </Wrapper>
            {showNotice && (
                <Alert
                    message="Be careful: Regular ingestion may override any unselected tags or terms that have been added to assets within DataHub."
                    type="warning"
                    style={{ marginTop: '8px' }}
                />
            )}
        </>
    );
};
