import { FolderOpen } from '@phosphor-icons/react';
import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import styled from 'styled-components';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { TYPE_ICON_CLASS_NAME, getSubTypeIcon } from '@app/entityV2/shared/components/subtypes';
import { getFirstSubType } from '@app/entityV2/shared/utils';

import { Container } from '@types';

const CONTAINER_ICON_SIZE = 14;

const IconWrapper = styled.span`
    line-height: 0;
    display: inline-flex;
    align-items: center;
`;

interface Props {
    container: Maybe<Container | GenericEntityProperties>;
}

export default function ContainerIcon({ container }: Props): JSX.Element {
    return (
        <IconWrapper>
            <ContainerIconBase container={container} />
        </IconWrapper>
    );
}

export function ContainerIconBase({ container }: Props): JSX.Element {
    const subtype = getFirstSubType(container)?.toLowerCase();
    return (
        (subtype && getSubTypeIcon(subtype, CONTAINER_ICON_SIZE)) || (
            <FolderOpen className={TYPE_ICON_CLASS_NAME} size={CONTAINER_ICON_SIZE} color="currentColor" />
        )
    );
}
