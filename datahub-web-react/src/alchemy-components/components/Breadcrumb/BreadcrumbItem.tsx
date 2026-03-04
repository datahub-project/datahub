import { debounce } from 'lodash';
import React, { useCallback } from 'react';

import { BreadcrumbButton, BreadcrumbItemWrapper, BreadcrumbLink } from '@components/components/Breadcrumb/components';
import { BreadcrumbItemType } from '@components/components/Breadcrumb/types';
import { Text } from '@components/components/Text';

interface Props {
    item: BreadcrumbItemType;
    updateIsTruncated?: (isTruncated: boolean) => void;
}

export function BreadcrumbItem({ item, updateIsTruncated }: Props) {
    const renderContent = useCallback(() => {
        if (item.href) {
            return <BreadcrumbLink to={item.href}>{item.label}</BreadcrumbLink>;
        }

        if (item.onClick) {
            return (
                <BreadcrumbButton size="sm" onClick={item.onClick}>
                    {item.label}
                </BreadcrumbButton>
            );
        }

        return (
            <Text type="span" size="sm" weight="medium">
                {item.label}
            </Text>
        );
    }, [item]);

    const handleResize: ResizeObserverCallback = useCallback(
        (entries) => {
            if (!entries || entries.length === 0) return;
            const node = entries[0].target;
            updateIsTruncated?.(node.scrollWidth > node.clientWidth);
        },
        [updateIsTruncated],
    );

    const measuredRef = useCallback(
        (node: HTMLDivElement | null) => {
            if (node !== null) {
                const resizeObserver = new ResizeObserver(debounce(handleResize, 100));
                resizeObserver.observe(node);
            }
        },
        [handleResize],
    );

    return (
        <BreadcrumbItemWrapper ref={measuredRef} $isActive={item.isActive}>
            {renderContent()}
        </BreadcrumbItemWrapper>
    );
}
