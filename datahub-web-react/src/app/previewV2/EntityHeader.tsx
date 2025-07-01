import { Tooltip } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { PreviewType } from '@app/entityV2/Entity';
import { DeprecationIcon } from '@app/entityV2/shared/components/styled/DeprecationIcon';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import StructuredPropertyBadge from '@app/entityV2/shared/containers/profile/header/StructuredPropertyBadge';
import { getNumberWithOrdinal } from '@app/entityV2/shared/utils';
import VersioningBadge from '@app/entityV2/shared/versioning/VersioningBadge';
import HealthIcon from '@app/previewV2/HealthIcon';
import SearchTextHighlighter from '@app/searchV2/matches/SearchTextHighlighter';
import { useEmbeddedProfileLinkProps } from '@app/shared/useEmbeddedProfileLinkProps';
import { getColor } from '@src/alchemy-components/theme/utils';

import { Deprecation, Health, Maybe } from '@types';

const EntityTitleContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 0.5rem;
    font-size: 16px;
    min-width: 15em;
`;

export const StyledLink = styled(Link)`
    min-width: 0;
`;

const EntityTitle = styled.div<{ $titleSizePx?: number }>`
    &&& {
        font-size: ${(props) => props.$titleSizePx || 16}px;
        font-weight: 700;
        vertical-align: middle;

        :hover {
            color: ${(p) => getColor('primary', 700, p.theme)};
        }
    }

    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    font-size: 13px;
    color: ${(p) => p.theme.styles['primary-color']};
    height: 100%;
`;

const CardEntityTitle = styled(EntityTitle)<{ $previewType?: Maybe<PreviewType> }>`
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
    previewData?: GenericEntityProperties | null;
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
    previewData,
}) => {
    const linkProps = useEmbeddedProfileLinkProps();

    return (
        <EntityTitleContainer>
            <StyledLink to={`${url}/`} {...linkProps}>
                {previewType === PreviewType.HOVER_CARD ? (
                    <Tooltip title={name}>
                        <CardEntityTitle onClick={onClick} $titleSizePx={titleSizePx} data-testid="entity-title">
                            {name || urn}
                        </CardEntityTitle>
                    </Tooltip>
                ) : (
                    <EntityTitle title={name} onClick={onClick} $titleSizePx={titleSizePx} data-testid="entity-title">
                        <SearchTextHighlighter field="name" text={name || urn} />
                    </EntityTitle>
                )}
            </StyledLink>
            {degree !== undefined && (
                <Tooltip
                    title={`This entity is a ${getNumberWithOrdinal(degree)} degree connection to ${
                        connectionName || 'the source entity'
                    }`}
                >
                    <DegreeText>{getNumberWithOrdinal(degree)}</DegreeText>
                </Tooltip>
            )}
            {deprecation?.deprecated && (
                <DeprecationIcon urn={urn} deprecation={deprecation} showUndeprecate showText={false} />
            )}
            {health && <HealthIcon urn={urn} health={health} baseUrl={url} />}
            <StructuredPropertyBadge structuredProperties={previewData?.structuredProperties} />
            <VersioningBadge versionProperties={previewData?.versionProperties ?? undefined} showPopover={false} />
        </EntityTitleContainer>
    );
};

export default EntityHeader;
