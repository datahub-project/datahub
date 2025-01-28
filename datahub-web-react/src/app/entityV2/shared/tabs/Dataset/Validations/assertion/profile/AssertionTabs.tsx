import React, { useState } from 'react';

import styled from 'styled-components';
import { Divider } from 'antd';
import { Tooltip } from '@components';

import { ANTD_GRAY } from '../../../../../constants';

const Tabs = styled.div`
    margin: 12px 24px;
    padding: 0px;
    display: flex;
    gap: 8px;
    border-radius: 4px;
    background-color: ${ANTD_GRAY[2]};
`;

const TabButton = styled.div<{ selected: boolean; disabled?: boolean }>`
    height: 40px;
    border-radius: 4px;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 0px 24px;
    border: none;
    background-color: ${(props) => (props.selected ? props.theme.styles['primary-color'] : 'transparent')};
    color: ${(props) => (props.selected ? '#fff' : ANTD_GRAY[9] || ANTD_GRAY[3])};
    font-size: 12px;
    cursor: pointer;
    ${(props) => props.disabled && 'opacity: 0.5;'}
    &:hover {
        cursor: ${(props) => (props.disabled ? 'not-allowed' : 'pointer')};
    }
`;

const TabContent = styled.div`
    padding: 12px 24px;
`;

const StyledDivider = styled(Divider)`
    margin: 12px 0px;
`;

type Props = {
    defaultSelectedTab: string;
    tabs: {
        key: string;
        label: React.ReactNode;
        content: React.ReactNode;
        disabled?: boolean;
        tooltip?: React.ReactNode;
    }[];
};

export const AssertionTabs = ({ defaultSelectedTab, tabs }: Props) => {
    const [selectedTab, setSelectedTab] = useState<string>(defaultSelectedTab);
    return (
        <>
            <Tabs>
                {tabs.map((tab) => (
                    <Tooltip title={tab.tooltip} placement="bottom" showArrow={false}>
                        <TabButton
                            selected={selectedTab === tab.key}
                            disabled={tab.disabled}
                            key={tab.key}
                            onClick={() => (!tab.disabled ? setSelectedTab(tab.key) : null)}
                        >
                            {tab.label}
                        </TabButton>
                    </Tooltip>
                ))}
            </Tabs>
            <StyledDivider />
            <TabContent>{tabs.find((tab) => tab.key === selectedTab)?.content}</TabContent>
        </>
    );
};
