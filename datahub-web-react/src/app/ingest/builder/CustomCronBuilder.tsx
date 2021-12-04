import { Input, Typography } from 'antd';
import React from 'react';

type Props = {
    updateCron: (newCron: string) => void;
};

export const CustomCronBuilder = ({ updateCron }: Props) => {
    return (
        <div style={{ width: 250 }}>
            <Typography.Text>Provide a custom cron schedule.</Typography.Text>
            <Input onChange={(e) => updateCron(e.target.value)} placeholder="* * * * *" />
        </div>
    );
};
