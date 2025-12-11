/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
/**
 * Generic Component that truncates text and shows a tooltip with the full text when hovered over using Ant Design's Tooltip component
 */
import { Tooltip } from '@components';
import { TooltipProps, Typography } from 'antd';
import React from 'react';

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
