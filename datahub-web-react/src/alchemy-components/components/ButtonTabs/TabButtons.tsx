import React, { useCallback } from 'react';

import { StyledTabButton, TabsWrapper } from '@components/components/ButtonTabs/components';
import { TabButtonsProps } from '@components/components/ButtonTabs/types';
import { tabButtonsDefaults } from '@components/components/ButtonTabs/utils';

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
        (event: React.MouseEvent<HTMLButtonElement>, key: string) => {
            // prevent event to not trigger validation of antd form
            event.stopPropagation();
            event.preventDefault();

            onTabClick(key);
        },
        [onTabClick],
    );

    return (
        <TabsWrapper className={className} data-testid={dataTestId} role="tablist" aria-label={ariaLabel} $fit={fit}>
            {tabs.map((tab) => {
                const isActive = tab.key === activeKey;
                return (
                    <StyledTabButton
                        $active={isActive}
                        $fit={fit}
                        onClick={(e) => onTabClickHandler(e, tab.key)}
                        variant="text"
                        key={tab.key}
                        type="button"
                        role="tab"
                        aria-selected={isActive}
                        data-testid={tab.dataTestId}
                    >
                        {tab.label}
                    </StyledTabButton>
                );
            })}
        </TabsWrapper>
    );
}
