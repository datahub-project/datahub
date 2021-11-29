import { Input, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

type Props = {
    updateCron: (newCron: string) => void;
};

const SelectTemplateHeader = styled(Typography.Title)`
    && {
        margin-bottom: 16px;
    }
`;

export const CustomCronBuilder = ({ updateCron }: Props) => {
    return (
        <>
            <SelectTemplateHeader level={5}>Custom Schedule</SelectTemplateHeader>
            <Typography.Text>Provide a custom cron schedule.</Typography.Text>
            <Input onChange={(e) => updateCron(e.target.value)} placeholder="* * * * *" />
        </>
    );
};
