import { Progress } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import useGetPromptInfo from '@app/entity/shared/containers/profile/sidebar/FormInfo/useGetPromptInfo';

const StyledProgress = styled(Progress)`
    &&& .ant-progress-outer {
        display: flex;
        flex-direction: column;
        align-items: flex-start;
        gap: 8px;
        align-self: stretch;
    }

    .ant-progress-bg {
        height: 4px !important;
    }
`;

interface Props {
    formUrn: string;
}
export default function ProgressBar({ formUrn }: Props) {
    const { totalRequiredSchemaFieldPrompts, numRequiredPromptsRemaining, requiredEntityPrompts } =
        useGetPromptInfo(formUrn);
    const totalRequiredPrompts = requiredEntityPrompts.length + totalRequiredSchemaFieldPrompts;
    const percent = ((totalRequiredPrompts - numRequiredPromptsRemaining) / totalRequiredPrompts) * 100;

    return (
        <StyledProgress
            percent={percent}
            showInfo={false}
            strokeColor="linear-gradient(270deg, #9f33cc 0%, #20d3bd 100%)"
            trailColor={ANTD_GRAY[1]}
        />
    );
}
