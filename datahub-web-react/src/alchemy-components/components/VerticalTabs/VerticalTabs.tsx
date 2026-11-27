import React, { useCallback, useRef } from 'react';
import { useTheme } from 'styled-components';

import { VerticalTab } from '@components/components/VerticalTabs/VerticalTab';
import { HiddenSvg, Rail } from '@components/components/VerticalTabs/components';
import { VerticalTabsProps } from '@components/components/VerticalTabs/types';

const NAVIGATION_KEYS = ['ArrowDown', 'ArrowUp', 'Home', 'End'];

// React 17 has no useId; a module counter keeps gradient ids unique when several rails render.
let gradientCounter = 0;

function useGradientId(): string {
    const idRef = useRef<string>();
    if (!idRef.current) {
        gradientCounter += 1;
        idRef.current = `vertical-tabs-selected-gradient-${gradientCounter}`;
    }
    return idRef.current;
}

function getNextEnabledIndex(enabled: boolean[], from: number, key: string): number {
    const count = enabled.length;
    if (key === 'Home') return enabled.indexOf(true);
    if (key === 'End') return enabled.lastIndexOf(true);
    const step = key === 'ArrowDown' ? 1 : -1;
    for (let offset = 1; offset <= count; offset++) {
        const index = (from + step * offset + count) % count;
        if (enabled[index]) return index;
    }
    return from;
}

export function VerticalTabs({
    items,
    activeKey,
    onChange,
    onTabClick,
    position = 'right',
    tooltipPlacement,
    id,
    className,
    'aria-label': ariaLabel,
    'data-testid': dataTestId,
}: VerticalTabsProps) {
    const theme = useTheme();
    const gradientId = useGradientId();
    const tabRefs = useRef<(HTMLButtonElement | null)[]>([]);

    const enabled = items.map((item) => !item.disabled);
    const activeIndex = items.findIndex((item) => item.key === activeKey && !item.disabled);
    // Roving tabindex: only one tab is in the page tab order; arrows move between the rest.
    const focusableIndex = activeIndex >= 0 ? activeIndex : enabled.indexOf(true);

    const handleClick = useCallback(
        (key: string) => {
            onTabClick?.(key);
            if (key !== activeKey) onChange?.(key);
        },
        [activeKey, onChange, onTabClick],
    );

    const handleKeyDown = (index: number) => (event: React.KeyboardEvent<HTMLButtonElement>) => {
        if (!NAVIGATION_KEYS.includes(event.key)) return;
        event.preventDefault();
        tabRefs.current[getNextEnabledIndex(enabled, index, event.key)]?.focus();
    };

    return (
        <Rail
            id={id}
            className={className}
            role="tablist"
            aria-orientation="vertical"
            aria-label={ariaLabel}
            data-testid={dataTestId}
            data-position={position}
        >
            <HiddenSvg aria-hidden focusable="false">
                <defs>
                    <linearGradient id={gradientId} x1="0%" y1="0%" x2="0%" y2="100%">
                        <stop offset="20%" stopColor={theme.colors.iconBrand} />
                        <stop offset="80%" stopColor={theme.colors.buttonFillBrand} />
                    </linearGradient>
                </defs>
            </HiddenSvg>
            {items.map((item, index) => (
                <VerticalTab
                    key={item.key}
                    ref={(node) => {
                        tabRefs.current[index] = node;
                    }}
                    item={item}
                    id={id ? `${id}-tab-${item.key}` : undefined}
                    isActive={index === activeIndex}
                    isFocusable={index === focusableIndex}
                    gradientId={gradientId}
                    tooltipPlacement={tooltipPlacement ?? (position === 'right' ? 'left' : 'right')}
                    onClick={handleClick}
                    onKeyDown={handleKeyDown(index)}
                />
            ))}
        </Rail>
    );
}
