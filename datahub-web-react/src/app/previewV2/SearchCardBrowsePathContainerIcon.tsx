import { FolderOpenOutlined } from '@ant-design/icons';
import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import styled from 'styled-components';
import { Entity } from '@types';
import { getSubTypeIcon } from '../entityV2/shared/components/subtypes';
import { useEntityRegistryV2 } from '../useEntityRegistry';
import { IconStyleType } from '../entityV2/Entity';

const IconWrapper = styled.span`
    img,
    svg {
        height: 12px;
    }

    margin-right: 6px;
`;

const DefaultIcon = styled(FolderOpenOutlined)`
    &&& {
        font-size: 14px;
    }
`;

interface Props {
    container: Maybe<Entity>;
}

function ContainerIcon({ container }: Props) {
    const entityRegistry = useEntityRegistryV2();
    if (!container) return null;

    const genericEntity = entityRegistry.getGenericEntityProperties(container.type, container);

    const subType = genericEntity?.subTypes?.typeNames?.[0].toLowerCase();
    const icon = getSubTypeIcon(subType) ||
        entityRegistry.getIcon(container.type, 16, IconStyleType.ACCENT, '#8d95b1') || <DefaultIcon />;

    return <IconWrapper>{icon}</IconWrapper>;
}

export default ContainerIcon;
