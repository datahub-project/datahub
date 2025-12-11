/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { TimelineContentDetails } from '@src/app/entityV2/shared/tabs/Incident/types';

export type TimelineItem = {
    key: string;
    content: React.ReactNode;
    dot?: React.ReactNode;
};

export interface BaseItemType {
    key: string;
}

export type TimelineProps<ItemType extends BaseItemType> = {
    items: ItemType[] | TimelineContentDetails[];
    renderContent: (item: ItemType) => React.ReactNode | JSX.Element;
    renderDot?: (item: ItemType) => React.ReactNode;
};
