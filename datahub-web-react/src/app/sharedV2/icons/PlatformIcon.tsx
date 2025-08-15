import ColorThief from 'colorthief';
import React, { useCallback, useRef, useState } from 'react';
import styled, { CSSObject, css } from 'styled-components/macro';

import { IconStyleType } from '@app/entityV2/Entity';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { getLighterRGBColor } from '@app/sharedV2/icons/colorUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { DataPlatform, EntityType } from '@types';

type PlatformIconProps = {
    platform: DataPlatform | null | undefined;
    size?: number;
    color?: string;
    alt?: string;
    entityType?: EntityType;
    styles?: CSSObject | undefined;
    title?: string;
    imageStyles?: CSSObject | undefined;
    className?: string;
    onError?: () => void;
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
    color,
    title,
    styles,
    imageStyles,
    className,
    onError,
}) => {
    const [background, setBackground] = useState<string | undefined>(undefined);
    const imgRef = useRef<HTMLImageElement>(null);
    const entityRegistry = useEntityRegistry();
    const logoUrl = platform?.properties?.logoUrl;

    const handleError = useCallback(() => {
        const img = imgRef.current;
        if (img) {
            img.removeAttribute('crossOrigin');
            setBackground(REDESIGN_COLORS.BACKGROUND_GREY);
        }
        onError?.();
    }, [onError, setBackground]);

    return (
        <IconContainer background={background} styles={styles} title={title} className={className}>
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
                            const [r, g, b] = colorThief.getColor(img, 25);
                            setBackground(`rgb(${getLighterRGBColor(r, g, b).join(', ')})`);
                        }
                    }}
                    onError={handleError}
                />
            ) : (
                entityRegistry.getIcon(entityType, size, IconStyleType.ACCENT, color)
            )}
        </IconContainer>
    );
};

export default PlatformIcon;
