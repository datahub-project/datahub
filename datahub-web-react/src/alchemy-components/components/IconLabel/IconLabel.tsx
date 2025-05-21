import React, { useEffect, useState } from 'react';

import { IconLabelContainer, ImageContainer, Label } from '@components/components/IconLabel/components';
import { IconLabelProps, IconType } from '@components/components/IconLabel/types';
import { isValidImageUrl } from '@components/components/IconLabel/utils';

export const IconLabel = ({ icon, name, type, style, imageUrl, testId }: IconLabelProps) => {
    const [isValidImage, setIsValidImage] = useState(false);

    useEffect(() => {
        if (type === IconType.IMAGE && typeof imageUrl === 'string') {
            isValidImageUrl(imageUrl).then(setIsValidImage); // Validate the image URL
        }
    }, [imageUrl, type]);

    const renderIcons = () => {
        if (type === IconType.ICON) {
            return icon;
        }

        if (type === IconType.IMAGE && isValidImage) {
            return <img alt={name} src={imageUrl} height={24} width={24} />;
        }

        return null; // Render the fallback (e.g., emoji or placeholder)
    };

    return (
        <IconLabelContainer>
            <ImageContainer data-testid={testId} style={style}>
                {renderIcons()}
            </ImageContainer>
            <Label data-testid={name} title={name}>
                {name}
            </Label>
        </IconLabelContainer>
    );
};
