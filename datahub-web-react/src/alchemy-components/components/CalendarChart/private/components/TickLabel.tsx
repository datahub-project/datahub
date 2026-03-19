import React, { SVGAttributes } from 'react';

type TickLabelProps = SVGAttributes<SVGTextElement> & {
    text: string;
};

export const TickLabel = ({ text, ...props }: TickLabelProps) => {
    return (
        <text {...props}>
            <tspan>{text}</tspan>
        </text>
    );
};
