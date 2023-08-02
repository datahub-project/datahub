/**
 * Generic Component that truncates text and shows a tooltip with the full text when hovered over using Ant Design's Tooltip component
 */

import React from 'react';
import { Tooltip, TooltipProps, Typography } from 'antd';

type Props = {
    text: string;
    maxLength: number;
    tooltipOptions?: TooltipProps;
    renderText?: (truncatedText: string) => React.ReactNode;
};

export const TruncatedTextWithTooltip = ({ text, maxLength, tooltipOptions, renderText, ...props }: Props) => {
    const truncatedText = text.length > maxLength ? `${text.substring(0, maxLength)}...` : text;

    return (
        <Tooltip title={text.length > maxLength ? text : undefined} {...tooltipOptions}>
            {renderText ? renderText(truncatedText) : <Typography.Text {...props}>{truncatedText}</Typography.Text>}
        </Tooltip>
    );
};
