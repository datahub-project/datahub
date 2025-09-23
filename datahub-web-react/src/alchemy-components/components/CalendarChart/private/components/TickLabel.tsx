import React, { SVGAttributes } from 'react';

export type TickLabelProps = SVGAttributes<SVGTextElement> & {
    text: string;
};

export const TickLabel = ({ text, ...props }: TickLabelProps) => {
    return (
        <text {...props}>
            <tspan>{text}</tspan>
        </text>
    );
};
