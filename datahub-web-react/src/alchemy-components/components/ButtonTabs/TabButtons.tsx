import React, { useCallback } from 'react';

import { StyledTabButton, TabTooltipAnchor, TabsWrapper } from '@components/components/ButtonTabs/components';
import { TabButtonsProps } from '@components/components/ButtonTabs/types';
import { tabButtonsDefaults } from '@components/components/ButtonTabs/utils';
import { Tooltip } from '@components/components/Tooltip';

export function TabButtons({
    tabs,
    activeTab: activeKey,
    onTabClick,
    fit = tabButtonsDefaults.fit,
    className,
    'data-testid': dataTestId,
    'aria-label': ariaLabel,
}: TabButtonsProps) {
    const onTabClickHandler = useCallback(
        (event: React.MouseEvent<HTMLButtonElement>, key: string, disabled?: boolean) => {
            // prevent event to not trigger validation of antd form
            event.stopPropagation();
            event.preventDefault();

            if (!disabled) onTabClick(key);
        },
        [onTabClick],
    );

    return (
        <TabsWrapper className={className} data-testid={dataTestId} role="tablist" aria-label={ariaLabel} $fit={fit}>
            {tabs.map((tab) => {
                const isActive = tab.key === activeKey;
                const button = (
                    <StyledTabButton
                        $active={isActive}
                        $fit={fit}
                        onClick={(e) => onTabClickHandler(e, tab.key, tab.disabled)}
                        variant="text"
                        key={tab.key}
                        type="button"
                        role="tab"
                        aria-selected={isActive}
                        disabled={tab.disabled}
                        data-testid={tab.dataTestId}
                    >
                        {tab.label}
                    </StyledTabButton>
                );
                if (!tab.tooltip) return button;
                return (
                    <Tooltip key={tab.key} title={tab.tooltip}>
                        <TabTooltipAnchor $fit={fit}>{button}</TabTooltipAnchor>
                    </Tooltip>
                );
            })}
        </TabsWrapper>
    );
}
