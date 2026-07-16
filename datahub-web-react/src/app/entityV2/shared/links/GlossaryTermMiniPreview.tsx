import { BookmarkSimple } from '@phosphor-icons/react/dist/csr/BookmarkSimple';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import GlossaryColoredIcon from '@app/glossaryV2/GlossaryColoredIcon';
import { getGlossaryTermColor, useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, GlossaryTerm } from '@types';

const GlossaryTermMiniPreviewContainer = styled.div`
    display: inline-flex;
    align-items: center;
    gap: 8px;
    flex-grow: 1;
    padding: 6px 10px;
    margin: 4px;
    height: 34px;
    border-radius: 5px;
    border: 1px solid ${(props) => props.theme.colors.border};
    background-color: ${(props) => props.theme.colors.bgSurface};
    color: ${(props) => props.theme.colors.textSecondary};
    cursor: pointer;
    font-family: Mulish;
    overflow: hidden;

    :hover {
        background-color: ${(props) => props.theme.colors.bgHover};
    }
`;

const GlossaryTermTitleText = styled.span`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

export const GlossaryTermMiniPreview = ({ glossaryTerm }: { glossaryTerm: GlossaryTerm }): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const { urn } = glossaryTerm;
    const url = entityRegistry.getEntityUrl(EntityType.GlossaryTerm, urn as string);
    const generateColor = useGenerateGlossaryColorFromPalette();
    const termColor = getGlossaryTermColor(glossaryTerm, generateColor);
    return (
        <Link to={url}>
            <HoverEntityTooltip entity={glossaryTerm} showArrow={false} placement="bottom">
                <GlossaryTermMiniPreviewContainer>
                    <GlossaryColoredIcon color={termColor} icon={BookmarkSimple} size={20} iconSize={12} />
                    <GlossaryTermTitleText>{glossaryTerm?.properties?.name}</GlossaryTermTitleText>
                </GlossaryTermMiniPreviewContainer>
            </HoverEntityTooltip>
        </Link>
    );
};
