/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
    typeColor: 'gray',
    typeColorLevel: 1800,
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
                typeColorLevel: 600,
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
