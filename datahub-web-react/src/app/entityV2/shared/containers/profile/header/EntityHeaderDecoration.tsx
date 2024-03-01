import React from 'react';
import styled from 'styled-components/macro';
import { generateColor } from '../../../components/styled/StyledTag';
import { DisplayProperties, EntityType } from "../../../../../../types.generated";

type Props = {
    urn: string;
    entityType: EntityType;
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
    displayProperties
}: Props) => {
    const glossaryColor = displayProperties?.colorHex || generateColor.hex(urn);

    if(entityType === EntityType.GlossaryNode || entityType === EntityType.GlossaryTerm)
    return <GlossaryItemRibbon color={glossaryColor}/>;

    return null;
};