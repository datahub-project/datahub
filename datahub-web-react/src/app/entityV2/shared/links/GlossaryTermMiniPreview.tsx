import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import * as Muicon from '@mui/icons-material';

import { EntityType, GlossaryTerm } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { HoverEntityTooltip } from '../../../recommendations/renderer/component/HoverEntityTooltip';
import { generateColorFromPalette } from '../../../glossaryV2/colorUtils';

const GlossaryTermMiniPreviewContainer = styled.div<{ color: string }>`
    display: inline-flex;
    flex-direction: column;
    flex-grow: 1;
    padding-left: 16px;
    padding-right: 12px;
    padding-top: 8px;
    // padding-bottom: 8px;
    margin: 4px;
    height: 34px;
    border-radius: 5px;
    justify-content: center;
    border: 1px solid #ccd1dd;
    background-color: #f8f8f8;
    :hover {
        background-color: #f2f2f2;
        // background-color: red;
    }
    color: #565657;
    cursor: pointer;
    font-family: Mulish;
    overflow: hidden;

    :after {
        content: '';
        width: 141.421%;
        background-color: ${(props) => props.color};
        min-height: 10px;
        position: relative;
        left: -64px;
        top: 20px;
        transform: rotate(-45deg);
        transform-origin: 1px 1px;
    }
`;

const GlossaryTermTitleText = styled.span`
    margin-bottom: 600px;
`;

export const GlossaryTermMiniPreview = ({ glossaryTerm }: { glossaryTerm: GlossaryTerm }): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const { parentNodes, urn } = glossaryTerm;
    const url = entityRegistry.getEntityUrl(EntityType.GlossaryTerm, urn as string);
    const lastParentNode = parentNodes && parentNodes.count > 0 && parentNodes.nodes[parentNodes.count - 1];
    const termColor = lastParentNode
        ? lastParentNode.displayProperties?.colorHex || generateColorFromPalette(lastParentNode.urn)
        : generateColorFromPalette(urn);
    const MaterialIcon = Muicon.Book;

    return (
        <Link to={url}>
            <HoverEntityTooltip entity={glossaryTerm} showArrow={false} placement="bottom">
                <GlossaryTermMiniPreviewContainer color={termColor}>
                    <span>
                        <MaterialIcon
                            style={{ verticalAlign: 'bottom', fontSize: 22, paddingRight: 6 }}
                            fontSize="large"
                            // sx={{ px: 1 }}
                        />
                        <GlossaryTermTitleText>{glossaryTerm?.properties?.name}</GlossaryTermTitleText>
                    </span>
                </GlossaryTermMiniPreviewContainer>
            </HoverEntityTooltip>
        </Link>
    );
};
