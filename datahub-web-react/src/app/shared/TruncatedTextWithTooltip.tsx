/**
 * Generic Component that truncates text and shows a tooltip with the full text when hovered over using Ant Design's Tooltip component
 */

import React from 'react';
import { TooltipProps, Typography } from 'antd';
import { Tooltip } from '@components';

type Props = {
    text: string;
    style?: React.CSSProperties;
    maxLength: number;
    tooltipOptions?: TooltipProps;
    renderText?: (truncatedText: string) => React.ReactNode;
};

export const TruncatedTextWithTooltip = ({ text, maxLength, tooltipOptions, renderText, ...props }: Props) => {
    const truncatedText = text.length > maxLength ? `${text.substring(0, maxLength)}...` : text;

    return (
        <Tooltip title={text.length > maxLength ? text : undefined} {...tooltipOptions}>
            {renderText ? (
                renderText(truncatedText)
            ) : (
                <Typography.Text style={props.style} {...props}>
                    {truncatedText}
                </Typography.Text>
            )}
        </Tooltip>
    );
};
