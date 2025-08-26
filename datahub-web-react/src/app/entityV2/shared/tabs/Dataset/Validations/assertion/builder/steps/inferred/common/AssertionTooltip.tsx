import { InfoCircleOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { Tooltip } from '@src/alchemy-components';
import { ANTD_GRAY } from '@src/app/entityV2/shared/constants';

const TooltipContainer = styled.div`
    & .ant-typography {
        color: white;
    }
`;

export interface AssertionTooltipProps {
    title: string;
    description: string;
}

export const AssertionTooltip = ({ title, description }: AssertionTooltipProps) => {
    return (
        <Tooltip
            color={ANTD_GRAY[9]}
            placement="right"
            title={
                <TooltipContainer>
                    <Typography.Paragraph>{title}</Typography.Paragraph>
                    <Typography.Text>{description}</Typography.Text>
                </TooltipContainer>
            }
        >
            <InfoCircleOutlined style={{ color: '#999' }} />
        </Tooltip>
    );
};
