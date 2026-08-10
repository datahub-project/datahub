import React, { useCallback, useState } from 'react';

import { TabButtons } from '@components/components/ButtonTabs/TabButtons';
import { TabContentWrapper } from '@components/components/ButtonTabs/components';
import { ButtonTabsProps } from '@components/components/ButtonTabs/types';
import {
    appendRenderedTabKey,
    getInitialActiveTabKey,
    shouldRenderTabPanel,
} from '@components/components/ButtonTabs/utils';

export function ButtonTabs({ tabs, defaultKey, onTabClick, fit, className }: ButtonTabsProps) {
    const [activeKey, setActiveKey] = useState<string | undefined>(() => getInitialActiveTabKey(tabs, defaultKey));
    const [renderedKeys, setRenderedKeys] = useState<string[]>(() => {
        const initial = getInitialActiveTabKey(tabs, defaultKey);
        return initial ? [initial] : [];
    });

    const onTabClickHandler = useCallback(
        (key: string) => {
            setActiveKey(key);
            setRenderedKeys((prev) => appendRenderedTabKey(prev, key));
            onTabClick?.(key);
        },
        [onTabClick],
    );

    return (
        <div className={className}>
            <TabButtons tabs={tabs} activeTab={activeKey} onTabClick={onTabClickHandler} fit={fit} />
            {tabs
                .filter((tab) => shouldRenderTabPanel(tab.key, activeKey, renderedKeys))
                .map((tab) => (
                    <TabContentWrapper $visible={tab.key === activeKey} key={tab.key}>
                        {tab.content}
                    </TabContentWrapper>
                ))}
        </div>
    );
}
