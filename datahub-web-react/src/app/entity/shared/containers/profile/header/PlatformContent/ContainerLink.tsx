import { FolderOpenOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Container, EntityType } from '@types';

const ContainerText = styled(Typography.Text)`
    font-size: 12px;
    line-height: 20px;
    color: ${ANTD_GRAY[7]};
`;

const ContainerIcon = styled(FolderOpenOutlined)`
    color: ${ANTD_GRAY[7]};

    &&& {
        font-size: 12px;
        margin-right: 4px;
    }
`;

const StyledLink = styled(Link)`
    white-space: nowrap;
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
            <ContainerIcon />
            <ContainerText>{containerName}</ContainerText>
        </StyledLink>
    );
}

export default ContainerLink;
