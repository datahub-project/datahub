/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Image } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { IconStyleType } from '@app/entity/Entity';
import { getPlatformName } from '@app/entity/shared/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity } from '@types';

const PreviewImage = styled(Image)`
    height: 16px;
    width: 16px;
    object-fit: contain;
    background-color: transparent;
`;

type Props = {
    entity: Entity;
};

const AutoCompleteEntityIcon = ({ entity }: Props) => {
    const entityRegistry = useEntityRegistry();

    const genericEntityProps = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const platformLogoUrl = genericEntityProps?.platform?.properties?.logoUrl;
    const platformName = getPlatformName(genericEntityProps);
    return (
        (platformLogoUrl && <PreviewImage preview={false} src={platformLogoUrl} alt={platformName || ''} />) ||
        entityRegistry.getIcon(entity.type, 12, IconStyleType.ACCENT)
    );
};

export default AutoCompleteEntityIcon;
