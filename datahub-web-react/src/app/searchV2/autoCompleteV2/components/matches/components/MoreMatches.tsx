import React from 'react';

import { MATCH_COLOR, MATCH_COLOR_LEVEL } from '@app/searchV2/autoCompleteV2/constants';
import { Text } from '@src/alchemy-components';
import { OverflowListItem } from '@src/app/shared/OverflowList';
import { pluralize } from '@src/app/shared/textUtil';

interface Props<Item extends OverflowListItem> {
    items: Item[];
}

export default function MoreMatches<Item extends OverflowListItem>({ items }: Props<Item>) {
    return (
        <Text
            type="span"
            color={MATCH_COLOR}
            colorLevel={MATCH_COLOR_LEVEL}
            size="sm"
        >{`+ ${items.length} more ${pluralize(items.length, 'match')}`}</Text>
    );
}
