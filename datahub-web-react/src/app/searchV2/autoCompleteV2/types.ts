import { FontColorLevelOptions, FontColorOptions, FontSizeOptions, FontWeightOptions } from '@components/theme/config';

export type EntityItemVariant = 'searchBar' | 'default';

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

    typeColor: FontColorOptions;
    typeColorLevel: FontColorLevelOptions;
};

export type VariantElementsPropsMapping = Map<EntityItemVariant, VariantProps>;
