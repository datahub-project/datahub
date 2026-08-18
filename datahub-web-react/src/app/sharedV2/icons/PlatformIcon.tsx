import ColorThief from 'colorthief';
import i18next from 'i18next';
import React, { useCallback, useRef, useState } from 'react';
import styled, { CSSObject, css, useTheme } from 'styled-components/macro';

import { IconStyleType } from '@app/entityV2/Entity';
import { PLATFORM_URN_TO_LOGO } from '@app/ingestV2/source/builder/constants';
import { getLighterRGBColor } from '@app/sharedV2/icons/colorUtils';
import LogicalPlatformDefaultIcon from '@app/sharedV2/logical/LogicalPlatformDefaultIcon';
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
    dataTestId?: string;
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
    alt = i18next.t('shared.misc:platformIcon.alt'),
    entityType = EntityType.DataPlatform,
    color,
    title,
    styles,
    imageStyles,
    className,
    onError,
    dataTestId,
}) => {
    const [background, setBackground] = useState<string | undefined>(undefined);
    const imgRef = useRef<HTMLImageElement>(null);
    const entityRegistry = useEntityRegistry();
    const theme = useTheme();
    // Prefer the platform's own logo URL when present, otherwise fall back
    // to the static asset registered under the platform URN in
    // PLATFORM_URN_TO_LOGO. This covers known platforms whose backend
    // metadata doesn't include a `logoUrl` (e.g. ingested-document source
    // platforms surfaced in the Context Documents sidebar).
    const logoUrl = platform?.properties?.logoUrl || (platform?.urn ? PLATFORM_URN_TO_LOGO[platform.urn] : undefined);

    const handleError = useCallback(() => {
        const img = imgRef.current;
        if (img) {
            img.removeAttribute('crossOrigin');
            setBackground(theme.colors.bgSurface);
        }
        onError?.();
    }, [onError, setBackground, theme.colors.bgSurface]);

    const defaultIcon = platform?.properties?.logical ? (
        <LogicalPlatformDefaultIcon size={size} />
    ) : (
        entityRegistry.getIcon(entityType, size, IconStyleType.ACCENT, color)
    );

    return (
        <IconContainer
            background={background}
            styles={styles}
            title={title}
            className={className}
            data-testid={dataTestId}
        >
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
                            // eslint-disable-next-line i18next/no-literal-string -- (untranslated-text) numeric rgb join separator
                            setBackground(`rgb(${getLighterRGBColor(r, g, b).join(', ')})`);
                        }
                    }}
                    onError={handleError}
                />
            ) : (
                defaultIcon
            )}
        </IconContainer>
    );
};

export default PlatformIcon;
