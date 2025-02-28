import React from 'react';
import styled from 'styled-components';
import { Tooltip } from '@components';
import { RecommendedFilter } from './types';
import { getFilterColor } from './utils';

const Pill = styled.div<{ color: string }>`
    border-radius: 20px;
    padding: 6px 10px;
    background-color: white;
    margin-right: 0px;
    border: 2px solid white;
    :hover {
        opacity: 1;
        cursor: pointer;
        border-color: ${(props) => props.color};
    }
    font-size: 14px;
    display: flex;
    align-items: center;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
`;

const Icon = styled.div`
    margin-right: 4px;
    &&&& {
        color: #ffffff;
    }
    display: flex;
`;

const Text = styled.div`
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
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
