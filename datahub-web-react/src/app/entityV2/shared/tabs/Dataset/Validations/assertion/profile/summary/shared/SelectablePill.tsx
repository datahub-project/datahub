import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

const Pill = styled.div<{ selected: boolean }>`
    display: flex;
    justify-content: center;
    align-items: center;
    border-radius: 20px;
    padding: 4px 12px;
    background-color: ${(props) =>
        props.selected ? props.theme.styles['primary-color'] : props.theme.colors.bgSurface};
    color: ${(props) => (props.selected ? props.theme.colors.textOnFillBrand : props.theme.colors.text)};
    :hover {
        opacity: 0.6;
        cursor: pointer;
    }
`;

type Props = {
    text: React.ReactNode;
    tip?: React.ReactNode;
    selected: boolean;
    onSelect: () => void;
};

export const SelectablePill = ({ text, tip, selected, onSelect }: Props) => {
    return (
        <Tooltip title={tip} placement="left" showArrow={false}>
            <Pill selected={selected} onClick={onSelect}>
                {text}
            </Pill>
        </Tooltip>
    );
};
