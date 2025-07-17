import React, { useCallback, useState } from 'react';
import styled from 'styled-components';

import { TabButtons } from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/assetTypeTabs/TabButtons';
import { Tab } from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/assetTypeTabs/types';

const TabContentWrapper = styled.div<{ $visible?: boolean }>`
    ${(props) => !props.$visible && 'display: none;'}
`;

interface Props {
    tabs: Tab[];
    defaultKey?: string;
    onTabClick: (key: string) => void;
}

export default function EntityTypeTabs({ tabs, defaultKey, onTabClick }: Props) {
    const [activeKey, setActiveKey] = useState<string | undefined>(defaultKey ?? tabs?.[0]?.key);
    const [renderedKeys, setRenderedKeys] = useState<string[]>(activeKey ? [activeKey] : []);

    const onTabClickHandler = useCallback(
        (key: string) => {
            setActiveKey(key);
            setRenderedKeys((prev) => [...new Set([...prev, key])]);
            onTabClick(key);
        },
        [onTabClick],
    );

    return (
        <>
            <TabButtons tabs={tabs} activeTab={activeKey} onTabClick={onTabClickHandler} />
            {tabs
                .filter((tab) => tab.key === activeKey || renderedKeys.includes(tab.key))
                .map((tab) => (
                    <TabContentWrapper $visible={tab.key === activeKey} key={tab.key}>
                        {tab.content}
                    </TabContentWrapper>
                ))}
        </>
    );
}
