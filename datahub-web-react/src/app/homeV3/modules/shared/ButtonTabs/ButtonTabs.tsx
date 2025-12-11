/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useCallback, useState } from 'react';
import styled from 'styled-components';

import { TabButtons } from '@app/homeV3/modules/shared/ButtonTabs/TabButtons';
import { Tab } from '@app/homeV3/modules/shared/ButtonTabs/types';

const TabContentWrapper = styled.div<{ $visible?: boolean }>`
    ${(props) => !props.$visible && 'display: none;'}
`;

interface Props {
    tabs: Tab[];
    defaultKey?: string;
    onTabClick?: (key: string) => void;
}

export default function ButtonTabs({ tabs, defaultKey, onTabClick }: Props) {
    const [activeKey, setActiveKey] = useState<string | undefined>(defaultKey ?? tabs?.[0]?.key);
    const [renderedKeys, setRenderedKeys] = useState<string[]>(activeKey ? [activeKey] : []);

    const onTabClickHandler = useCallback(
        (key: string) => {
            setActiveKey(key);
            setRenderedKeys((prev) => [...new Set([...prev, key])]);
            onTabClick?.(key);
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
