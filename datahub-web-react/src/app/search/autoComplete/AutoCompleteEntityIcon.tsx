import React from 'react';
import styled from 'styled-components';
import { Image } from 'antd';
import { Entity } from '../../../types.generated';
import { getPlatformName } from '../../entity/shared/utils';
import { useEntityRegistry } from '../../useEntityRegistry';
import { IconStyleType } from '../../entity/Entity';

const PreviewImage = styled(Image)`
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

type Props = {
    entity: Entity;
    scale: number;
};

const DEFAULT_ICON_SIZE = 12;
const DEFAULT_PREVIEW_IMAGE_SIZE = 22;

const AutoCompleteEntityIcon = ({ entity, scale }: Props) => {
    const entityRegistry = useEntityRegistry();

    const iconFontSize = Math.floor(DEFAULT_ICON_SIZE * scale);
    const previewImageSize = Math.floor(DEFAULT_PREVIEW_IMAGE_SIZE * scale);

    const genericEntityProps = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const platformLogoUrl = genericEntityProps?.platform?.properties?.logoUrl;
    const platformName = getPlatformName(genericEntityProps);
    const icon =
        (platformLogoUrl && (
            <PreviewImage
                preview={false}
                src={platformLogoUrl}
                alt={platformName || ''}
                style={{
                    width: previewImageSize,
                    height: previewImageSize,
                }}
            />
        )) ||
        entityRegistry.getIcon(entity.type, iconFontSize, IconStyleType.ACCENT);

    return icon;
};

export default AutoCompleteEntityIcon;
