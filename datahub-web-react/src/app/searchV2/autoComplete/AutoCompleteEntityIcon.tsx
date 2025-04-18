import React from 'react';
import styled from 'styled-components';
import { Image } from 'antd';
import { Entity } from '../../../types.generated';
import { getPlatformName } from '../../entity/shared/utils';
import { useEntityRegistry } from '../../useEntityRegistry';
import { IconStyleType } from '../../entity/Entity';

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
