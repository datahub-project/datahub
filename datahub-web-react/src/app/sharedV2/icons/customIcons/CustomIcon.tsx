// this component can be use for added custom icon by just passing the svg file path
import React from 'react';
import { useTranslation } from 'react-i18next';

interface Props {
    iconSvg: string;
}

export const CustomIcon = ({ iconSvg }: Props) => {
    const { t } = useTranslation('shared.misc');
    return (
        <div>
            <img src={iconSvg} alt={t('customIcon.alt')} />
        </div>
    );
};
