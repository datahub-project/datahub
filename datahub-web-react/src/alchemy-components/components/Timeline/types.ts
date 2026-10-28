import React from 'react';

import { TimelineContentDetails } from '@src/app/entityV2/shared/tabs/Incident/types';

export interface BaseItemType {
    key: string;
}

export type TimelineProps<ItemType extends BaseItemType> = {
    items: ItemType[] | TimelineContentDetails[];
    renderContent: (item: ItemType) => React.ReactNode | JSX.Element;
    renderDot?: (item: ItemType) => React.ReactNode;
};
