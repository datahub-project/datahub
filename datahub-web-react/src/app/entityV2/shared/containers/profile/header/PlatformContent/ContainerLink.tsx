import React from 'react';
import styled from 'styled-components';
import { Link } from 'react-router-dom';
import { Maybe } from 'graphql/jsutils/Maybe';
import { Container, EntityType } from '../../../../../../../types.generated';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import ContainerIcon from './ContainerIcon';
import { ANTD_GRAY } from '../../../../constants';
import { useEmbeddedProfileLinkProps } from '../../../../../../shared/useEmbeddedProfileLinkProps';

const ContainerText = styled.span`
    font-size: 14px;
    line-height: 20px;
    margin-left: 6px;
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
    const linkProps = useEmbeddedProfileLinkProps();

    if (!container) return null;

    const containerUrl = entityRegistry.getEntityUrl(EntityType.Container, container.urn);
    const containerName = entityRegistry.getDisplayName(EntityType.Container, container);

    return (
        <StyledLink to={containerUrl} data-testid="container" {...linkProps}>
            <ContainerIcon container={props.container} />
            <ContainerText>{containerName}</ContainerText>
        </StyledLink>
    );
}

export default ContainerLink;
