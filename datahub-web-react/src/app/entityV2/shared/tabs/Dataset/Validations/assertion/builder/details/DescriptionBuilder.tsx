import React from 'react';
import Typography from 'antd/lib/typography';
import styled from 'styled-components';
import { Input } from 'antd';

const Section = styled.div``;

type Props = {
    value?: string | null;
    onChange: (newValue: string) => void;
    disabled?: boolean;
};

export const DescriptionBuilder = ({ value, onChange, disabled }: Props) => {
    const updateDescription = (description: string) => {
        onChange(description);
    };

    return (
        <Section>
            <Typography.Title level={5}>Name</Typography.Title>
            <Input.TextArea
                value={value || ''}
                onChange={(e) => updateDescription(e.target.value)}
                disabled={disabled}
                placeholder="Optional. If not specified, a name will be generated from the settings."
            />
        </Section>
    );
};
