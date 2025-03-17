import React, { useEffect, useState } from 'react';
import { IconLabelProps, IconType } from './types';
import { IconLabelContainer, ImageContainer, Label } from './components';
import { isValidImageUrl } from './utils';

export const IconLabel = ({ icon, name, type, style, imageUrl }: IconLabelProps) => {
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
            <ImageContainer style={style}>{renderIcons()}</ImageContainer>
            <Label title={name}>{name}</Label>
        </IconLabelContainer>
    );
};
