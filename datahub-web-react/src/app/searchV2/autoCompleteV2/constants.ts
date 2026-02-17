import { VariantElementsPropsMapping, VariantProps } from '@app/searchV2/autoCompleteV2/types';

export const DEFAULT_STYLES: VariantProps = {
    showEntityPopover: true,
    nameWeight: 'semiBold',
    nameCanBeHovered: true,
    nameFontSize: 'md',
};

export const VARIANT_STYLES: VariantElementsPropsMapping = new Map([
    ['default', DEFAULT_STYLES],
    [
        'searchBar',
        {
            ...DEFAULT_STYLES,
            showEntityPopover: false,
            nameCanBeHovered: false,
            nameWeight: 'normal',
        },
    ],
    [
        'select',
        {
            ...DEFAULT_STYLES,
            nameCanBeHovered: false,
        },
    ],
]);
