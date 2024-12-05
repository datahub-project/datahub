import React from 'react';
import styled from 'styled-components';

const Bar = styled.div<{ status: number; bar: number; isFieldSelected: boolean; size: string }>`
    width: ${(props) => (props.size === 'default' ? '5px' : '3px')};
    height: ${(props) => 5 * (props.bar + 1)}px;
    background: ${(props) => {
        return props.bar <= props.status ? '#533fd1' : '#C6C0E0';
    }};
    opacity: ${(props) => (props.isFieldSelected && !(props.bar <= props.status) ? '0.5' : '')};
    margin-right: ${(props) => (props.size === 'default' ? '3px' : '4px')};
    border-radius: 20px;
`;

const BarContainer = styled.div`
    display: flex;
    flex-direction: row;
    align-items: flex-end;
`;

type PopularityBarsProps = {
    status: number;
    isFieldSelected?: boolean;
    size?: string;
};
export const PopularityBars = ({ status, isFieldSelected, size = 'default' }: PopularityBarsProps) => {
    const renderBars = () => {
        const bars: any = [];
        for (let bar = 1; bar <= 3; bar++) {
            bars.push(
                <Bar
                    className="usage-bars"
                    key={bar}
                    status={status}
                    bar={bar}
                    isFieldSelected={!!isFieldSelected}
                    size={size}
                />,
            );
        }
        return bars;
    };

    return <BarContainer>{renderBars()}</BarContainer>;
};
