import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { Container, EntityType } from '../../types.generated';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';
import { useEntityRegistry } from '../useEntityRegistry';
import SearchCardBrowsePathContainerIcon from './SearchCardBrowsePathContainerIcon';

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
    min-width: 50px;
`;

const StyledLink = styled(Link)`
    white-space: nowrap;
    border-radius: 4px;
    overflow: hidden;
    text-overflow: ellipsis;
    display: flex;
    color: ${REDESIGN_COLORS.LINK_GREY};
    :hover {
        color: ${REDESIGN_COLORS.TITLE_PURPLE};
    }
`;

interface Props {
    container: Maybe<Container>;
}

function ContainerLink(props: Props) {
    const { container } = props;
    const entityRegistry = useEntityRegistry();

    if (!container) return null;

    const containerUrl = entityRegistry.getEntityUrl(EntityType.Container, container.urn);
    const containerName = entityRegistry.getDisplayName(EntityType.Container, container);

    return (
        <Path>
            <StyledLink to={containerUrl} data-testid="container">
                <SearchCardBrowsePathContainerIcon container={props.container} />
                <ContainerText title={containerName}>{containerName}</ContainerText>
            </StyledLink>
        </Path>
    );
}

export default ContainerLink;
