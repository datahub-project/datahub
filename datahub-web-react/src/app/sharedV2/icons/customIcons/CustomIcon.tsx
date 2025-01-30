// this component can be use for added custom icon by just passing the svg file path
import React from 'react';

interface Props {
    iconSvg: string;
}

export const CustomIcon = ({ iconSvg }: Props) => {
    return (
        <div>
            <img src={iconSvg} alt="Icon" />
        </div>
    );
};
