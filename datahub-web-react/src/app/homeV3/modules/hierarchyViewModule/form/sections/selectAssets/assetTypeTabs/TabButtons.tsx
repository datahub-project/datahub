import { Button, colors } from '@components';
import React, { useCallback } from 'react';
import styled from 'styled-components';

import { Tab } from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/assetTypeTabs/types';

const StyledButton = styled(Button)<{ $active?: boolean }>`
    width: 100%;
    justify-content: center;

    ${(props) =>
        props.$active
            ? `
        background: ${colors.white};
        :hover {
            background: ${colors.white};
        }
    `
            : `
        color: ${colors.gray[500]} !important;
    `}
`;

const TabsWrapper = styled.div`
    display: flex;
    padding: 2px;
    background: ${colors.gray[1500]};
    border-radius: 6px;
`;

interface Props {
    tabs: Tab[];
    activeTab: string | undefined;
    onTabClick: (key: string) => void;
}

export function TabButtons({ tabs, activeTab: activeKey, onTabClick }: Props) {
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
        <TabsWrapper>
            {tabs.map((tab) => {
                const isActive = tab.key === activeKey;
                return (
                    <StyledButton $active={isActive} onClick={(e) => onTabClickHandler(e, tab.key)} variant="text">
                        {tab.label}
                    </StyledButton>
                );
            })}
        </TabsWrapper>
    );
}
