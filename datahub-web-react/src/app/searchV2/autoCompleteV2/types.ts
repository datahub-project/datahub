import { FontColorLevelOptions, FontColorOptions } from '@components/theme/config';

export type EntityItemVariant = 'searchBar' | 'default';

export type EntityItemStyles = {
    nameColor: FontColorOptions;
    nameColorLevel: FontColorLevelOptions;
    subtitleColor: FontColorOptions;
    subtitleColorLevel: FontColorLevelOptions;
    matchColor: FontColorOptions;
    matchColorLevel: FontColorLevelOptions;
    typeColor: FontColorOptions;
    typeColorLevel: FontColorLevelOptions;
};

export type VariantStylesMapping = Map<EntityItemVariant, EntityItemStyles>;
