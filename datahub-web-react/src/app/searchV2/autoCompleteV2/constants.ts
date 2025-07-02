import { FontColorLevelOptions, FontColorOptions } from '@components/theme/config';

import { EntityItemStyles, VariantStylesMapping } from '@app/searchV2/autoCompleteV2/types';

export const NAME_COLOR: FontColorOptions = 'gray';
export const NAME_COLOR_LEVEL: FontColorLevelOptions = 600;

export const SUBTITLE_COLOR: FontColorOptions = 'gray';
export const SUBTITLE_COLOR_LEVEL: FontColorLevelOptions = 600;

export const MATCH_COLOR: FontColorOptions = 'gray';
export const MATCH_COLOR_LEVEL: FontColorLevelOptions = 1700;

export const TYPE_COLOR: FontColorOptions = 'gray';
export const TYPE_COLOR_LEVEL: FontColorLevelOptions = 600;

export const DEFAULT_STYLES: EntityItemStyles = {
    nameColor: 'gray',
    nameColorLevel: 600,
    subtitleColor: 'gray',
    subtitleColorLevel: 1800,
    matchColor: 'gray',
    matchColorLevel: 1700,
    typeColor: 'gray',
    typeColorLevel: 600,
};

export const VARIANT_STYLES: VariantStylesMapping = new Map([
    ['default', DEFAULT_STYLES],
    [
        'searchBar',
        {
            ...DEFAULT_STYLES,
            ...{
                nameColorLevel: 600,
                subtitleColorLevel: 600,
                matchColorLevel: 1700,
                typeColorLevel: 600,
            },
        },
    ],
]);
