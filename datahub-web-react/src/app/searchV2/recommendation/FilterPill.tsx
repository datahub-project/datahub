import React from 'react';
import styled from 'styled-components';
import { Tooltip, colors } from '@components';
import { RecommendedFilter } from './types';
import { getFilterColor } from './utils';

const Pill = styled.div<{ color: string }>`
    border-radius: 20px;
    padding: 4px 8px;
    background-color: ${colors.gray[1600]};
    :hover {
        opacity: 1;
        cursor: pointer;
        background-color: ${colors.gray[100]};
    }
    font-size: 14px;
    display: flex;
    align-items: center;
    gap: 4px;
`;

const Icon = styled.div`
    &&&& {
        color: #ffffff;
    }
    display: flex;
`;

const Text = styled.div`
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
    color: ${colors.gray[900]};
`;

type Props = {
    filter: RecommendedFilter;
    onToggle: () => void;
};

export const FilterPill = ({ filter, onToggle }: Props) => {
    const color = getFilterColor(filter.field, filter.value);
    return (
        <Tooltip
            showArrow={false}
            placement="top"
            title={
                <>
                    View results in <b>{filter.label}</b>
                </>
            }
        >
            <Pill onClick={onToggle} color={color}>
                {filter.icon && <Icon>{filter.icon}</Icon>}
                <Text>{filter.label}</Text>
            </Pill>
        </Tooltip>
    );
};
