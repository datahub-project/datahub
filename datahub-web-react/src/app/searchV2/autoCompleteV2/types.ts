import { FontSizeOptions, FontWeightOptions } from '@components/theme/config';

export type EntityItemVariant = 'searchBar' | 'default' | 'select';

export type VariantProps = {
    showEntityPopover: boolean;

    nameWeight: FontWeightOptions;
    nameCanBeHovered: boolean;
    nameFontSize: FontSizeOptions;
};

export type VariantElementsPropsMapping = Map<EntityItemVariant, VariantProps>;
