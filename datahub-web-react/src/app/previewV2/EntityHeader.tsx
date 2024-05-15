import React from 'react';
import { Link } from 'react-router-dom';
import { Tooltip, Typography } from 'antd';
import styled from 'styled-components';
import SearchTextHighlighter from '../searchV2/matches/SearchTextHighlighter';
import { PreviewType } from '../entityV2/Entity';
import { Deprecation, Health, Maybe } from '../../types.generated';
import { REDESIGN_COLORS, SEARCH_COLORS } from '../entityV2/shared/constants';
import { DeprecationPill } from '../entityV2/shared/components/styled/DeprecationPill';
import HealthIcon from './HealthIcon';
import { isUnhealthy } from '../shared/health/healthUtils';
import { PageRoutes } from '../../conf/Global';
import { getNumberWithOrdinal } from '../entityV2/shared/utils';

const EntityTitleContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 0.5rem;
    width: 100%;
`;

const StyledLink = styled(Link)`
    display: block;
    max-width: 75%;
`;

const EntityTitle = styled(Typography.Text) <{ $titleSizePx?: number }>`
    display: block;

    &&& {
        font-size: ${(props) => props.$titleSizePx || 16}px;
        font-weight: 700;
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

const CardEntityTitle = styled(EntityTitle) <{ $previewType?: Maybe<PreviewType> }>`
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: ${(props) => (props.$previewType === PreviewType.HOVER_CARD ? `100px` : '250px')};
`;

const DegreeText = styled.div`
    border-radius: 18px;
    background: ${REDESIGN_COLORS.COLD_GREY_TEXT_BLUE_1};
    padding: 3px 5px;
    font-size: 12px;
    font-weight: 700;
    width: fit-content;
    color: ${REDESIGN_COLORS.SUB_TEXT};
`;

interface EntityHeaderProps {
    name: string;
    onClick?: () => void;
    previewType?: Maybe<PreviewType>;
    titleSizePx?: number;
    url: string;
    urn: string;
    deprecation: Deprecation | null | undefined;
    health: Health[] | undefined;
    degree?: number;
    connectionName?: Maybe<string>;
}

const EntityHeader: React.FC<EntityHeaderProps> = ({
    name,
    onClick,
    previewType,
    titleSizePx,
    url,
    urn,
    deprecation,
    health,
    degree,
    connectionName,
}) => {
    const isEmbeddedProfile = window.location.pathname.startsWith(PageRoutes.EMBED);
    const linkProps = isEmbeddedProfile ? { target: '_blank', rel: 'noreferrer noopener' } : {};

    return (
        <EntityTitleContainer>
            <StyledLink to={url} {...linkProps}>
                {previewType === PreviewType.HOVER_CARD ? (
                    <CardEntityTitle
                        onClick={onClick}
                        ellipsis={{ tooltip: { title: name, showArrow: false, mouseEnterDelay: 0.5 } }}
                        $titleSizePx={titleSizePx}
                    >
                        {name || ''}
                    </CardEntityTitle>
                ) : (
                    <EntityTitle onClick={onClick} $titleSizePx={titleSizePx}>
                        <SearchTextHighlighter field="name" text={name || ''} />
                    </EntityTitle>
                )}
            </StyledLink>
            {degree !== undefined && (
                <Tooltip
                    title={`This entity is a ${getNumberWithOrdinal(degree)} degree connection to ${connectionName || 'the source entity'
                        }`}
                >
                    <DegreeText>{getNumberWithOrdinal(degree)}</DegreeText>
                </Tooltip>
            )}
            {deprecation?.deprecated && <DeprecationPill urn={urn} deprecation={deprecation} showUndeprecate />}
            {health && isUnhealthy(health) && <HealthIcon health={health} baseUrl={url} />}
        </EntityTitleContainer>
    );
};

export default EntityHeader;
