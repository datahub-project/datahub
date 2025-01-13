import React from 'react';
import { IconLabelProps, IconType } from './types';
import { IconLabelContainer, ImageContainer, Label } from './component';
import { isValidImageUrl } from './utils';

export const IconLabel = ({ icon, name, type, style, imageUrl }: IconLabelProps) => {
    const renderIcons = () => {
        if (type === IconType.ICON) {
            return icon;
        }

        if (type === IconType.IMAGE && typeof imageUrl === 'string' && isValidImageUrl(imageUrl)) {
            return <img alt={name} src={imageUrl} height={24} width={24} />;
        }

        return null; // Render the fallback (e.g., emoji)
    };

    return (
        <IconLabelContainer>
            <ImageContainer style={style}>{renderIcons()}</ImageContainer>
            <Label title={name}>{name}</Label>
        </IconLabelContainer>
    );
};
