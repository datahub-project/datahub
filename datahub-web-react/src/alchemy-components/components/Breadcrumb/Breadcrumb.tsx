import React, { useCallback, useMemo, useState } from 'react';

import { BreadcrumbItem } from '@components/components/Breadcrumb/BreadcrumbItem';
import { BreadcrumbItemContainer, NoShrinkIcon, Wrapper } from '@components/components/Breadcrumb/components';
import { breadcrumbDefaults } from '@components/components/Breadcrumb/defaults';
import { BreadcrumbProps } from '@components/components/Breadcrumb/types';
import { Popover } from '@components/components/Popover';

export const Breadcrumb = ({ items, showPopover = breadcrumbDefaults.showPopover }: BreadcrumbProps) => {
    const defaultSeparator = (
        <NoShrinkIcon icon="CaretRight" source="phosphor" color="gray" colorLevel={1800} size="sm" />
    );

    const [itemsTruncationState, setItemsTruncationState] = useState<Map<string, boolean>>(new Map());

    const hasAnyTruncatedItem = useMemo(
        () => [...itemsTruncationState.values()].includes(true),
        [itemsTruncationState],
    );

    const shouldShowPopover = useMemo(() => showPopover && hasAnyTruncatedItem, [hasAnyTruncatedItem, showPopover]);

    const updateTruncationState = useCallback((itemKey: string, isTruncated: boolean) => {
        setItemsTruncationState((prev) => {
            // do not update the map if there are no changes
            if (prev.get(itemKey) === isTruncated) return prev;

            const newMap = new Map(prev);
            newMap.set(itemKey, isTruncated);
            return newMap;
        });
    }, []);

    return (
        <Popover content={shouldShowPopover ? <Breadcrumb items={items} showPopover={false} /> : null}>
            <Wrapper>
                {items.map((item, index) => {
                    const isLast = index === items.length - 1;

                    return (
                        <BreadcrumbItemContainer key={item.key}>
                            <BreadcrumbItem
                                item={item}
                                updateIsTruncated={(isTruncated) => updateTruncationState(item.key, isTruncated)}
                            />
                            {!isLast && <>{item.separator || defaultSeparator}</>}
                        </BreadcrumbItemContainer>
                    );
                })}
            </Wrapper>
        </Popover>
    );
};
