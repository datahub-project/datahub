import React from 'react';
import styled from 'styled-components/macro';
import { DisplayProperties } from '../../../../../../types.generated';
import { generateColorFromPalette } from '../../../../../glossaryV2/colorUtils';
import { GenericEntityProperties } from '../../../../../entity/shared/types';

type Props = {
    urn: string;
    entityData: GenericEntityProperties | null;
    displayProperties?: DisplayProperties;
};

interface GlossaryItemRibbonProps {
    color: string;
}

export const GLOSSARY_RIBBON_SIZE = 8;

const GlossaryItemRibbon = styled.span<GlossaryItemRibbonProps>`
    position: absolute;
    left: -20px;
    top: 4px;
    width: 80px;
    transform: rotate(-45deg);
    padding: ${GLOSSARY_RIBBON_SIZE}px;
    opacity: 1;
    background-color: ${(props) => `${props.color}`};
`;

export const GlossaryPreviewCardDecoration = ({ urn, entityData, displayProperties }: Props) => {
    const parentNodeCount = entityData?.parentNodes?.count || 0;
    const urnText = parentNodeCount === 0 ? urn : entityData?.parentNodes?.nodes[parentNodeCount - 1]?.urn || '';
    const glossaryColor = displayProperties?.colorHex || generateColorFromPalette(urnText);

    return <GlossaryItemRibbon color={glossaryColor} />;
};
