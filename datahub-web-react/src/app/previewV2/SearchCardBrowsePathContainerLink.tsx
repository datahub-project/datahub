import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { Entity } from '@types';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';
import { useEntityRegistry } from '../useEntityRegistry';
import SearchCardBrowsePathContainerIcon from './SearchCardBrowsePathContainerIcon';
import useEmbeddedProfileLinkProps from '../shared/useEmbeddedProfileLinkProps';

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
    max-width: 150px;
`;

const StyledLink = styled(Link)`
    white-space: nowrap;
    border-radius: 4px;
    overflow: hidden;
    text-overflow: ellipsis;
    display: flex;
    line-height: 10px;
    color: ${REDESIGN_COLORS.LINK_GREY};
    :hover {
        color: ${REDESIGN_COLORS.TITLE_PURPLE};
    }
`;

interface Props {
    container: Maybe<Entity>;
}

function ContainerLink(props: Props) {
    const { container } = props;
    const entityRegistry = useEntityRegistry();
    const linkProps = useEmbeddedProfileLinkProps();

    if (!container) return null;

    const containerUrl = entityRegistry.getEntityUrl(container.type, container.urn);
    const containerName = entityRegistry.getDisplayName(container.type, container);

    return (
        <Path>
            <StyledLink to={containerUrl} data-testid="container" {...linkProps}>
                <SearchCardBrowsePathContainerIcon container={container} />
                <ContainerText title={containerName}>{containerName}</ContainerText>
            </StyledLink>
        </Path>
    );
}

export default ContainerLink;
