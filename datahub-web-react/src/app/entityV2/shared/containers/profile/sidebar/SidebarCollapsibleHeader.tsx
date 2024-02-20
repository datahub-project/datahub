import React, { useContext } from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import EntitySidebarContext from '../../../../../shared/EntitySidebarContext';
import { REDESIGN_COLORS } from '../../../constants';
import { EntitySidebarTab } from '../../../types';

const Controls = styled.div<{ isCollapsed: boolean }>`
    display: flex;
    align-items: center;
    justify-content: ${(props) => (props.isCollapsed ? 'center' : 'space-between')};
    height: 56px;
    padding: 8px 20px 5px 20px;
    border-bottom: 1px solid #d5d5d5;
`;

const Title = styled.div`
    display: flex;
    flex-direction: column;
    align-items: start;
    justify-content: center;
    gap: 2px;
`;

const TabTitle = styled(Typography.Text)`
    font-size: 14px;
    font-weight: 800;
    line-height: 20px;
    color: ${REDESIGN_COLORS.HEADING_COLOR};
`;

const TitleDescription = styled(Typography.Text)`
    font-size: 10px;
    font-weight: 400;
    color: ${REDESIGN_COLORS.HEADING_COLOR};
    opacity: 0.5;
`;

interface Props {
    currentTab?: EntitySidebarTab;
}

export default function SidebarCollapsibleHeader({ currentTab }: Props) {
    const { isClosed } = useContext(EntitySidebarContext);

    const currentTabName = currentTab?.name === 'About' ? 'Summary' : currentTab?.name;
    const currentTabDescription = currentTab?.description;

    return (
        <Controls isCollapsed={isClosed}>
            {!isClosed && currentTab && (
                <Title>
                    <TabTitle>{currentTabName}</TabTitle>
                    {currentTabDescription && <TitleDescription> {currentTabDescription}</TitleDescription>}
                </Title>
            )}
        </Controls>
    );
}
