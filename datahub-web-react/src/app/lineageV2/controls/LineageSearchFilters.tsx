import { SearchOutlined } from '@ant-design/icons';
import React from 'react';
import { Input } from 'antd';
import { Panel } from 'reactflow';
import styled from 'styled-components';

const StyledControlsPanel = styled(Panel)<{ isRootPanelExpanded: boolean }>`
    margin-top: 36px;
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    padding: 16px;
    width: 255px;
    border-radius: 8px;
    border: 1px solid #d5d5d5;
    background: #fff;
    box-shadow: 0px 4px 4px 0px rgba(224, 224, 224, 0.25);
    transition: width 0.3s ease-in-out;
    overflow: hidden;
    margin-left: ${({ isRootPanelExpanded }) => (isRootPanelExpanded ? '178px' : '66px')};
    transition: margin-left 0.3s ease-in-out;
`;

const GlobalFiltersTitleText = styled.div`
    font-size: 14px;
    font-weight: 600;
    color: #0958d9;
`;

const GlobalFilterSubText = styled.div`
    color: #595959;
    font-size: 10px;
    font-weight: 500;
`;

const StyledSearchInput = styled(Input)`
    margin-top: 19px;
`;

type Props = {
    isRootPanelExpanded: boolean;
};

const LineageSearchFilters = ({ isRootPanelExpanded }: Props) => {
    return (
        <StyledControlsPanel position="top-left" isRootPanelExpanded={isRootPanelExpanded}>
            <GlobalFiltersTitleText>Global Filters</GlobalFiltersTitleText>
            <GlobalFilterSubText>These can be used to filter across all nodes</GlobalFilterSubText>

            <StyledSearchInput placeholder="Search by name..." suffix={<SearchOutlined />} />
        </StyledControlsPanel>
    );
};

export default LineageSearchFilters;
