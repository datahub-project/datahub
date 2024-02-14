import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { Container, EntityType } from '../../types.generated';
import { ANTD_GRAY, SEARCH_COLORS } from '../entityV2/shared/constants';
import { useEntityRegistry } from '../useEntityRegistry';
import SearchCardBrowsePathContainerIcon from './SearchCardBrowsePathContainerIcon';

const ContainerText = styled.span`
    color: ${SEARCH_COLORS.PLATFORM_TEXT};
    font-family: Mulish;
    font-size: 13px;
    font-style: normal;
    font-weight: 500;
    line-height: normal;
    max-width: 100px;
    display: inline-block;
    overflow: hidden;
    text-overflow: ellipsis;
    line-height: 0.8;
    min-width: 50px
`;

const StyledLink = styled(Link)`
    white-space: nowrap;
    :hover {
        background-color: ${ANTD_GRAY[3]};
    }
    border-radius: 4px;
    overflow: hidden;
    text-overflow: ellipsis;
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
        <StyledLink to={containerUrl} data-testid="container">
            <SearchCardBrowsePathContainerIcon container={props.container} />
            <ContainerText title={containerName}>{containerName}</ContainerText>
        </StyledLink>
    );
}

export default ContainerLink;
