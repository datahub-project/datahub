/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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

    typeColor: FontColorOptions;
    typeColorLevel: FontColorLevelOptions;
};

export type VariantElementsPropsMapping = Map<EntityItemVariant, VariantProps>;
