import { VariantElementsPropsMapping, VariantProps } from '@app/searchV2/autoCompleteV2/types';

export const DEFAULT_STYLES: VariantProps = {
    showEntityPopover: true,
    nameColor: 'gray',
    nameColorLevel: 600,
    nameWeight: 'semiBold',
    nameCanBeHovered: true,
    nameFontSize: 'md',
    subtitleColor: 'gray',
    subtitleColorLevel: 1800,
    matchColor: 'gray',
    matchColorLevel: 1700,
};

export const VARIANT_STYLES: VariantElementsPropsMapping = new Map([
    ['default', DEFAULT_STYLES],
    [
        'searchBar',
        {
            ...DEFAULT_STYLES,
            ...{
                showEntityPopover: false,
                nameCanBeHovered: false,
                nameColorLevel: 600,
                nameWeight: 'normal',
                subtitleColorLevel: 600,
                matchColorLevel: 1700,
            },
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
