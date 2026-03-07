import * as phosphorIcons from '@phosphor-icons/react';

export const getIconComponent = (icon: string) => {
    return phosphorIcons[icon];
};
