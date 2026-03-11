import * as phosphorIcons from '@phosphor-icons/react';

const MUI_TO_PHOSPHOR: Record<string, string> = {
    MoreVert: 'DotsThreeVertical',
    ChevronRight: 'CaretRight',
    ChevronLeft: 'CaretLeft',
    Close: 'X',
    Add: 'Plus',
    Delete: 'Trash',
    ContentCopy: 'Copy',
    Edit: 'PencilSimple',
    Search: 'MagnifyingGlass',
    ArrowBack: 'ArrowLeft',
    ArrowDownward: 'ArrowDown',
    ArrowForward: 'ArrowRight',
    Visibility: 'Eye',
    VisibilityOff: 'EyeSlash',
    WarningAmber: 'Warning',
    ErrorOutline: 'WarningCircle',
    KeyboardArrowUp: 'CaretUp',
    KeyboardArrowDown: 'CaretDown',
    AccountCircle: 'UserCircle',
    AutoMode: 'ArrowsClockwise',
};

export const getIconComponent = (icon: string) => {
    const direct = phosphorIcons[icon];
    if (direct) return direct;

    const mapped = MUI_TO_PHOSPHOR[icon];
    if (mapped) {
        if (process.env.NODE_ENV === 'development') {
            console.warn(`Icon "${icon}" is a legacy MUI name. Use "${mapped}" instead.`);
        }
        return phosphorIcons[mapped];
    }

    return undefined;
};
