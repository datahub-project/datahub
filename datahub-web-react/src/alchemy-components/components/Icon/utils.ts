import { IconSource } from '@components/components/Icon/types';
import * as materialIcons from '@mui/icons-material';
import * as phosphorIcons from 'phosphor-react';

export const getIconNames = () => {
    // We only want "Filled" (mui default) and "Outlined" icons
    const filtered = Object.keys(materialIcons).filter(
        (key) =>
            !key.includes('Filled') && !key.includes('TwoTone') && !key.includes('Rounded') && !key.includes('Sharp'),
    );

    const filled: string[] = [];
    const outlined: string[] = [];

    filtered.forEach((key) => {
        if (key.includes('Outlined')) {
            outlined.push(key);
        } else if (!key.includes('Outlined')) {
            filled.push(key);
        }
    });

    return {
        filled,
        outlined,
    };
};

export const getIconComponent = (source: IconSource, icon: string) => {
    return source === 'phosphor' ? phosphorIcons[icon] : materialIcons[icon];
};
