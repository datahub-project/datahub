/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { OverflowListItem } from '@src/app/sharedV2/OverflowList';

export interface LinkAttributes {
    url: string;
    label: string;
    onClick?: () => void;
    className?: string;
}

export interface LinkItem extends OverflowListItem {
    url: string;
    description: string;
    attributes: LinkAttributes;
}
