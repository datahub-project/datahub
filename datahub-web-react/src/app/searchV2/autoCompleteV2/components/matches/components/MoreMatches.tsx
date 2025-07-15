import React from 'react';

import { FontColorLevelOptions, FontColorOptions } from '@components/theme/config';

import { Text } from '@src/alchemy-components';
import { OverflowListItem } from '@src/app/shared/OverflowList';
import { pluralize } from '@src/app/shared/textUtil';

interface Props<Item extends OverflowListItem> {
    items: Item[];
    color?: FontColorOptions;
    colorLevel?: FontColorLevelOptions;
}

export default function MoreMatches<Item extends OverflowListItem>({ items, color, colorLevel }: Props<Item>) {
    return (
        <Text
            type="span"
            color={color}
            colorLevel={colorLevel}
            size="sm"
        >{`+ ${items.length} more ${pluralize(items.length, 'match')}`}</Text>
    );
}
