import React from 'react';
import styled from 'styled-components/macro';
import { DisplayProperties, EntityType } from '../../../../../../types.generated';
import { generateColorFromPalette } from '../../../../../glossaryV2/colorUtils';

type Props = {
    urn: string;
    entityType: EntityType;
    entityData: any;
    displayProperties?: DisplayProperties;
};

interface GlossaryItemRibbonProps {
    color: string;
}

const GlossaryItemRibbon = styled.span<GlossaryItemRibbonProps>`
    position: absolute;
    left: -20px;
    top: 4px;
    width: 80px;
    transform: rotate(-45deg);
    padding: 8px;
    opacity: 1;
    background-color: ${(props) =>  `${props.color}`};
`;

export const EntityHeaderDecoration = ({
    urn,
    entityType,
    entityData,
    displayProperties
}: Props) => {
    const parentNodeCount = entityData?.parentNodes?.count || 0;
    const urnText = parentNodeCount===0?urn : entityData.parentNodes.nodes[parentNodeCount-1].urn;
    const glossaryColor = displayProperties?.colorHex || generateColorFromPalette(urnText);

    if(entityType === EntityType.GlossaryNode || entityType === EntityType.GlossaryTerm)
    return <GlossaryItemRibbon color={glossaryColor}/>;

    return null;
};