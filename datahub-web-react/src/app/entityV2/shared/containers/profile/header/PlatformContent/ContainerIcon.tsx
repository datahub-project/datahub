/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { FolderOpenOutlined } from '@ant-design/icons';
import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import styled from 'styled-components';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { TYPE_ICON_CLASS_NAME, getSubTypeIcon } from '@app/entityV2/shared/components/subtypes';
import { getFirstSubType } from '@app/entityV2/shared/utils';

import { Container } from '@types';

const IconWrapper = styled.span`
    line-height: 0;
    .${TYPE_ICON_CLASS_NAME} {
        font-size: 14px;
    }
`;

const DefaultIcon = styled(FolderOpenOutlined)``;

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
    return (subtype && getSubTypeIcon(subtype)) || <DefaultIcon />;
}
