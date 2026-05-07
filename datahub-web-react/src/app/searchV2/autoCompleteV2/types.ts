import { FontColorLevelOptions, FontColorOptions, FontSizeOptions, FontWeightOptions } from '@components/theme/config';

export type EntityItemVariant = 'searchBar' | 'default' | 'select';

export type VariantProps = {
    showEntityPopover: boolean;

    nameColor: FontColorOptions;
    nameColorLevel: FontColorLevelOptions;
    nameWeight: FontWeightOptions;
    nameCanBeHovered: boolean;
    nameFontSize: FontSizeOptions;

    subtitleColor: FontColorOptions;
    subtitleColorLevel: FontColorLevelOptions;

    matchColor: FontColorOptions;
    matchColorLevel: FontColorLevelOptions;
};

export type VariantElementsPropsMapping = Map<EntityItemVariant, VariantProps>;
