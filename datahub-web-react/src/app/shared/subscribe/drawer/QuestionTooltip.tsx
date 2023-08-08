import React from 'react';
import { QuestionCircleOutlined } from '@ant-design/icons';
import { Tooltip } from 'antd';
import { ANTD_GRAY } from '../../../entity/shared/constants';

interface Props {
    tooltipTitle: string;
}

export default function QuestionTooltip({ tooltipTitle }: Props) {
    return (
        <Tooltip title={tooltipTitle}>
            <QuestionCircleOutlined style={{ color: ANTD_GRAY[7] }} />
        </Tooltip>
    );
}
