import React, { useRef, useState } from 'react';
import styled, { css, CSSObject } from 'styled-components/macro';
import ColorThief from 'colorthief';
import { DataPlatform, EntityType } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { IconStyleType } from '../../entityV2/Entity';
import { getLighterRGBColor } from './colorUtils';

type PlatformIconProps = {
    platform: DataPlatform | null | undefined;
    size?: number;
    alt?: string;
    entityType?: EntityType;
    styles?: CSSObject | undefined;
    title?: string;
    imageStyles?: CSSObject | undefined;
};

const IconContainer = styled.div<{ background?: string; styles: CSSObject | undefined }>`
    display: flex;
    align-items: center;
    justify-content: center;
    width: auto;
    padding: 6px;
    border-radius: 8px;
    background-color: ${(props) => props.background || 'transparent'};
    ${({ styles }) => (styles ? css(styles) : undefined)};
`;

const PreviewImage = styled.img<{ size: number; imageStyles?: CSSObject | undefined }>`
    height: ${(props) => props.size}px;
    width: ${(props) => props.size}px;
    min-width: ${(props) => props.size}px;
    object-fit: contain;
    background-color: transparent;
    ${({ imageStyles }) => (imageStyles ? css(imageStyles) : undefined)};
`;

const PlatformIcon: React.FC<PlatformIconProps> = ({
    platform,
    size = 17,
    alt = 'Platform Logo',
    entityType = EntityType.DataPlatform,
    title,
    styles,
    imageStyles,
}) => {
    const [background, setBackground] = useState<string | undefined>(undefined);
    const imgRef = useRef<HTMLImageElement>(null);
    const entityRegistry = useEntityRegistry();
    const logoUrl = platform?.properties?.logoUrl;
    return (
        <IconContainer background={background} styles={styles} title={title}>
            {logoUrl ? (
                <PreviewImage
                    crossOrigin="anonymous"
                    ref={imgRef}
                    src={logoUrl}
                    alt={alt}
                    size={size}
                    imageStyles={imageStyles}
                    onLoad={() => {
                        const img = imgRef.current;
                        if (img && img.width > 0 && img.height > 0) {
                            const colorThief = new ColorThief();
                            img.crossOrigin = 'anonymous';
                            const [r, g, b] = colorThief.getColor(img, 25);
                            setBackground(`rgb(${getLighterRGBColor(r, g, b).join(', ')})`);
                        }
                    }}
                />
            ) : (
                entityRegistry.getIcon(entityType, size, IconStyleType.ACCENT)
            )}
        </IconContainer>
    );
};

export default PlatformIcon;
