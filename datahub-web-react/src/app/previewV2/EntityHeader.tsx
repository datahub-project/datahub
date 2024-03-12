import React from 'react';
import { Link } from 'react-router-dom';
import { Typography } from 'antd';
import styled from 'styled-components';
import SearchTextHighlighter from '../searchV2/matches/SearchTextHighlighter';
import { PreviewType } from '../entityV2/Entity';
import { Maybe } from '../../types.generated';
import { REDESIGN_COLORS, SEARCH_COLORS } from '../entityV2/shared/constants';

const EntityTitleContainer = styled.div`
    display: flex;
    align-items: center;
    width: 75%;
`;

const StyledLink = styled(Link)`
    display: block;
`;

const EntityTitle = styled(Typography.Text)<{ $titleSizePx?: number }>`
    display: block;
    &&& {
        margin-right 8px;
        font-size: ${(props) => props.$titleSizePx || 16}px;
        font-weight: 700;
        line-height: 16px;
        vertical-align: middle;
        :hover {
            color: ${REDESIGN_COLORS.HOVER_PURPLE};
        }
    }
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    font-size: 13px;
    color: ${SEARCH_COLORS.TITLE_PURPLE};
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
