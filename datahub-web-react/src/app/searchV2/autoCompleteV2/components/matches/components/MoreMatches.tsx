import React from 'react';
import { useTranslation } from 'react-i18next';

import { FontColorLevelOptions, FontColorOptions } from '@components/theme/config';

import { Text } from '@src/alchemy-components';
import { OverflowListItem } from '@src/app/shared/OverflowList';

interface Props<Item extends OverflowListItem> {
    items: Item[];
    color?: FontColorOptions;
    colorLevel?: FontColorLevelOptions;
}

export default function MoreMatches<Item extends OverflowListItem>({ items, color, colorLevel }: Props<Item>) {
    const { t } = useTranslation('search');

    return (
        <Text type="span" color={color} colorLevel={colorLevel} size="sm">
            {t('autoComplete.moreMatchesCount', { count: items.length })}
        </Text>
    );
}
