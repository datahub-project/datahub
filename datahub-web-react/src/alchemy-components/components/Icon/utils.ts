import * as materialIcons from '@mui/icons-material';

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

export const getIconComponent = (icon: string) => {
    return materialIcons[icon];
};
