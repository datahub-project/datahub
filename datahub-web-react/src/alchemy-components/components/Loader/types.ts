/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { SizeOptions } from '@src/alchemy-components/theme/config';

export type JustifyContentOptions = 'center' | 'flex-start';

export type AlignItemsOptions = 'center' | 'flex-start' | 'none';

export type LoaderProps = {
    size?: SizeOptions;
    justifyContent?: JustifyContentOptions;
    alignItems?: AlignItemsOptions;
    padding?: number;
};
