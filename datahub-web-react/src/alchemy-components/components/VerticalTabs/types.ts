import React from 'react';

import type { TooltipPlacement } from '@components/components/Tooltip';

export type VerticalTabItem = {
    key: string;
    /** Phosphor icon component, rendered with `regular` weight (or `fill` while active). */
    icon: React.ComponentType<any>;
    /** Icon to show instead of `icon` while the tab is active. */
    selectedIcon?: React.ComponentType<any>;
    /** Short caption rendered under the icon. Omit for an icon-only item. */
    label?: string;
    /**
     * Tooltip shown on hover. When omitted, the label is shown as a tooltip only if it is
     * truncated.
     */
    tooltip?: React.ReactNode;
    /** Accessible name. Defaults to `label`, then to `tooltip` when it is a string. */
    ariaLabel?: string;
    disabled?: boolean;
    dataTestId?: string;
};

export type VerticalTabsPosition = 'left' | 'right';

export type VerticalTabsProps = {
    items: VerticalTabItem[];
    /** Key of the active tab. Pass `undefined` to render no tab as active (e.g. a collapsed panel). */
    activeKey?: string;
    /** Called when an enabled tab other than the active one is clicked. */
    onChange?: (key: string) => void;
    /** Called on every click of an enabled tab, including the active one (e.g. to toggle a panel). */
    onTabClick?: (key: string) => void;
    /** Which side of its panel the rail sits on. Sets the default tooltip placement. */
    position?: VerticalTabsPosition;
    tooltipPlacement?: TooltipPlacement;
    /** Also used as a prefix for each tab's DOM id: `${id}-tab-${key}`. */
    id?: string;
    className?: string;
    'aria-label'?: string;
    'data-testid'?: string;
};
