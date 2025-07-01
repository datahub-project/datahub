import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import ContextPathEntityIcon from '@app/previewV2/ContextPathEntityIcon';
import { useEmbeddedProfileLinkProps } from '@app/shared/useEmbeddedProfileLinkProps';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity } from '@types';

const Path = styled.div`
    white-space: nowrap;
    font-size: 13px;
    font-style: normal;
    font-weight: 500;
    text-overflow: ellipsis;
    overflow: hidden;
    display: flex;
    align-items: center;
`;

const ContainerText = styled.span`
    display: inline-block;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 150px; // TODO: Remove in favor of smart truncation
`;

const StyledLink = styled(Link)<{ $disabled?: boolean; $color: string }>`
    white-space: nowrap;
    border-radius: 4px;
    overflow: hidden;
    text-overflow: ellipsis;
    display: flex;
    gap: 4px;
    align-items: center;
    line-height: 22px;
    color: ${(props) => props.$color};

    && svg {
        color: ${(props) => props.$color};
    }

    :hover {
        color: ${({ $disabled, $color, theme }) => ($disabled ? $color : theme.styles['primary-color'])};
        cursor: ${({ $disabled }) => ($disabled ? 'default' : 'pointer')};

        && svg {
            color: ${({ $disabled, $color, theme }) => ($disabled ? $color : theme.styles['primary-color'])};
        }
    }
`;

interface Props {
    entity: Maybe<Entity>;
    linkDisabled?: boolean;
    style?: React.CSSProperties;
    color?: string;
}

function ContextPathEntityLink(props: Props) {
    const { entity, linkDisabled, style, color } = props;
    const entityRegistry = useEntityRegistry();
    const linkProps = useEmbeddedProfileLinkProps();

    if (!entity) return null;

    const containerUrl = entityRegistry.getEntityUrl(entity.type, entity.urn);
    const containerName = entityRegistry.getDisplayName(entity.type, entity);

    return (
        <Path style={style}>
            <StyledLink
                to={linkDisabled ? null : containerUrl}
                data-testid="container"
                $disabled={linkDisabled}
                $color={color ?? REDESIGN_COLORS.LINK_GREY}
                {...linkProps}
            >
                <ContextPathEntityIcon entity={entity} />
                <ContainerText title={containerName}>{containerName}</ContainerText>
            </StyledLink>
        </Path>
    );
}

export default ContextPathEntityLink;
