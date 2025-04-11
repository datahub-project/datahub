import { Popover } from '@components';
import React from 'react';
import styled from 'styled-components';
import { EntityType } from '@src/types.generated';
import { usePropagationContextEntities, PropagationContext } from '../tags/usePropagationContextEntities';
import PropagationEntityLink from './PropagationEntityLink';
import { PropagateThunderbolt, PropagateThunderboltFilled } from './PropagationIcon';
import { PropagationRelationshipType } from './utils';

const PopoverWrapper = styled.div`
    display: flex;
    flex-direction: column;
`;

const PopoverTitle = styled.div`
    font-weight: bold;
    font-size: 14px;
    padding: 6px 0px;
    color: #eeecfa;
`;

const PopoverDescription = styled.div`
    max-width: 340px;
    font-size: 14px;
    color: #eeecfa;
    display: inline;
    padding: 0px 0px 8px 0px;
`;

const PopoverAttributes = styled.div`
    display: flex;
`;

const PopoverAttribute = styled.div`
    margin-right: 12px;
    margin-bottom: 4px;
`;

const PopoverAttributeTitle = styled.div`
    font-size: 14px;
    color: #eeecfa;
    font-weight: bold;
    margin: 8px 0px;
    overflow: hidden;
    text-overflow: ellipsis;
`;

interface Props {
    entityType: EntityType;
    context?: string | null;
}

export default function LabelPropagationDetails({ entityType, context }: Props) {
    const contextObj = context ? (JSON.parse(context) as PropagationContext) : null;
    const isPropagated = contextObj?.propagated;
    const { originEntity } = usePropagationContextEntities(contextObj);
    const isSiblingsRelationship = contextObj?.relationship === PropagationRelationshipType.SIBLINGS;

    if (!isPropagated || !originEntity || (EntityType.GlossaryTerm !== entityType && entityType !== EntityType.Tag))
        return null;

    const popoverContent = originEntity ? (
        <PopoverWrapper>
            <PopoverDescription>
                This {EntityType.GlossaryTerm === entityType ? 'Glossary Term' : 'Tag'} was automatically propagated
                from {isSiblingsRelationship ? 'a sibling' : 'an upstream'}.{' '}
            </PopoverDescription>
            <PopoverAttributes>
                {originEntity && (
                    <PopoverAttribute>
                        <PopoverAttributeTitle>Origin</PopoverAttributeTitle>
                        <PropagationEntityLink entity={originEntity} />
                    </PopoverAttribute>
                )}
            </PopoverAttributes>
        </PopoverWrapper>
    ) : undefined;

    return (
        <Popover
            overlayInnerStyle={{ backgroundColor: '#272D48' }}
            showArrow={false}
            title={
                <PopoverTitle>
                    <PropagateThunderboltFilled fontSize={16} />
                    Propagated {EntityType.GlossaryTerm === entityType ? 'Glossary Term' : 'Tag'}
                </PopoverTitle>
            }
            content={popoverContent}
        >
            <PropagateThunderbolt fontSize={14} data-testid="glossaryTermPropagationIndicator" />
        </Popover>
    );
}
