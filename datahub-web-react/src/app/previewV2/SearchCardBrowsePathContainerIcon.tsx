import { FolderOpenOutlined } from '@ant-design/icons';
import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import styled from 'styled-components';
import { Container, Dashboard, Dataset } from '../../types.generated';
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
    container: Maybe<Container> | Maybe<Dashboard> | Maybe<Dataset>;
}

function ContainerIcon({ container }: Props) {
    const entityRegistry = useEntityRegistryV2();
    if (!container) return null;
    const subType = container?.subTypes?.typeNames?.[0].toLowerCase();
    const type = container?.type;

    return (
        <IconWrapper>
            {(subType && getSubTypeIcon(subType)) ||
                entityRegistry.getIcon(type, 16, IconStyleType.ACCENT, '#8d95b1') || <DefaultIcon />}
        </IconWrapper>
    );
}

export default ContainerIcon;
