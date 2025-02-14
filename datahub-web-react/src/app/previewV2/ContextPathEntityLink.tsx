import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { Entity } from '@types';
import { colors } from '@src/alchemy-components';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';
import { useEntityRegistry } from '../useEntityRegistry';
import ContextPathEntityIcon from './ContextPathEntityIcon';
import { useEmbeddedProfileLinkProps } from '../shared/useEmbeddedProfileLinkProps';

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

const StyledLink = styled(Link)<{ $disabled?: boolean }>`
    white-space: nowrap;
    border-radius: 4px;
    overflow: hidden;
    text-overflow: ellipsis;
    display: flex;
    gap: 4px;
    align-items: center;
    line-height: 22px;
    color: ${REDESIGN_COLORS.LINK_GREY};

    && svg {
        color: ${REDESIGN_COLORS.LINK_GREY};
    }

    :hover {
        color: ${({ $disabled }) => ($disabled ? REDESIGN_COLORS.LINK_GREY : colors.violet[500])};

        && svg {
            color: ${({ $disabled }) => ($disabled ? REDESIGN_COLORS.LINK_GREY : colors.violet[500])};
        }
    }
`;

interface Props {
    entity: Maybe<Entity>;
    linkDisabled?: boolean;
    style?: React.CSSProperties;
}

function ContextPathEntityLink(props: Props) {
    const { entity, linkDisabled, style } = props;
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
                {...linkProps}
            >
                <ContextPathEntityIcon entity={entity} />
                <ContainerText title={containerName}>{containerName}</ContainerText>
            </StyledLink>
        </Path>
    );
}

export default ContextPathEntityLink;
