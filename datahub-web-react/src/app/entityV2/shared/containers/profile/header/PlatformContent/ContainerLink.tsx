/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import ContainerIcon from '@app/entityV2/shared/containers/profile/header/PlatformContent/ContainerIcon';
import { useEmbeddedProfileLinkProps } from '@app/shared/useEmbeddedProfileLinkProps';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Container, EntityType } from '@types';

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
