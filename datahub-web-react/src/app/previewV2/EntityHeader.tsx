import React from 'react';
import { Link } from 'react-router-dom';
import { Typography } from 'antd';
import styled from 'styled-components';
import SearchTextHighlighter from '../searchV2/matches/SearchTextHighlighter';
import { PreviewType } from '../entityV2/Entity';
import { Maybe } from '../../types.generated';
import { SEARCH_COLORS } from '../entityV2/shared/constants';

const EntityTitleContainer = styled.div`
    display: flex;
    align-items: center;
    width: 75%;
`;

const StyledLink = styled(Link)`
    display: block;
    width: 100%;
`;

const EntityTitle = styled(Typography.Text)<{ $titleSizePx?: number }>`
    display: block;
    &&&:hover {
        text-decoration: underline;
    }
    &&& {
        margin-right 8px;
        font-size: ${(props) => props.$titleSizePx || 16}px;
        font-weight: 700;
        vertical-align: middle;
    }
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    font-family: Manrope;
    font-size: 13px;
    color: ${SEARCH_COLORS.LINK_BLUE};
    width: 75%;
    height: 100%;
`;

const CardEntityTitle = styled(EntityTitle)<{ $previewType?: Maybe<PreviewType> }>`
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: ${(props) => (props.$previewType === PreviewType.HOVER_CARD ? `100px` : '250px')};
`;

interface EntityHeaderProps {
    name: string;
    onClick?: () => void;
    previewType?: Maybe<PreviewType>;
    titleSizePx?: number;
    url: string;
}

const EntityHeader: React.FC<EntityHeaderProps> = ({ name, onClick, previewType, titleSizePx, url }) => (
    <EntityTitleContainer>
        <StyledLink to={url}>
            {previewType === PreviewType.HOVER_CARD ? (
                <CardEntityTitle onClick={onClick} $titleSizePx={titleSizePx}>
                    {name || ' '}
                </CardEntityTitle>
            ) : (
                <EntityTitle onClick={onClick} $titleSizePx={titleSizePx}>
                    <SearchTextHighlighter field="name" text={name || ''} />
                </EntityTitle>
            )}
        </StyledLink>
    </EntityTitleContainer>
);

export default EntityHeader;
