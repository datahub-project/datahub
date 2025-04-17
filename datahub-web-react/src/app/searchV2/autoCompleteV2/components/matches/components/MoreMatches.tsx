import React from 'react';

import { Text } from '@src/alchemy-components';
import { OverflowListItem } from '@src/app/shared/OverflowList';
import { pluralize } from '@src/app/shared/textUtil';

interface Props<Item extends OverflowListItem> {
    items: Item[];
}

export default function MoreMatches<Item extends OverflowListItem>({ items }: Props<Item>) {
    return <Text color="gray">{`+ ${items.length} more ${pluralize(items.length, 'match')}`}</Text>;
}
