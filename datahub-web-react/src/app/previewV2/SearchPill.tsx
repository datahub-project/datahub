import { Tooltip } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { pluralize } from '../shared/textUtil';

type Props = {
    icon: any;
    count: number;
    label: string;
    onClick?: (e: React.MouseEvent) => void;
    enabled?: boolean;
    countLabel: string;
    active?: boolean;
};

const PillContainer = styled.div<{ enabled?: boolean; active?: boolean }>`
    height: 22px;
    padding-left: 8px;
    padding-right: 8px;
    background-color: #e8e6eb;
    background-color: ${({ active }) => (active ? '#d4d3d7' : '#e8e6eb')};
    cursor: pointer;
    border-radius: 11px;
    text-align: center;
    display: flex;
    flex-direction: row;
    justify-content: center;
    align-items: center;
    gap: 4px;
    color: ${({ enabled }) => (enabled ? '#3F54D1' : '#b0a2c2')};
    border: 1px solid #b0a2c200;
    :hover {
        border: 1px solid ${({ enabled }) => (enabled ? '#3F54D1' : '#b0a2c200')};
    }
    & svg {
        font-size: 12px;
        // color: #b0a2c2;
        color: ${({ enabled }) => (enabled ? '#3F54D1' : '#b0a2c2')};
        fill: ${({ enabled }) => (enabled ? '#3F54D1' : '#b0a2c2')};
    }

    font-family: Mulish;
    font-size: 9px;
    font-style: normal;
    font-weight: 400;
    line-height: normal;
`;

// pluralize

const SearchPill = ({ icon, onClick, enabled, label, count, countLabel, active }: Props) => {
    return (
        <Tooltip
            title={`${count} ${pluralize(count, countLabel, countLabel === 'match' ? 'es' : 's')}`}
            showArrow={false}
        >
            <PillContainer active={active} enabled={enabled} onClick={onClick}>
                {icon} {label} {count}
            </PillContainer>
        </Tooltip>
    );
};

export default SearchPill;
