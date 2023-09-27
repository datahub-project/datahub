import React from 'react';
import Typography from 'antd/lib/typography';
import styled from 'styled-components';
import { Form, Input } from 'antd';

const Section = styled.div`
    margin: 16px 0 24px;
`;

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
            <Typography.Title level={5}>Description</Typography.Title>
            <Form.Item name="description" rules={[{ required: true, message: 'Required' }]}>
                <Input.TextArea
                    value={value || ''}
                    onChange={(e) => updateDescription(e.target.value)}
                    placeholder="Give your assertion a human-readable description."
                    disabled={disabled}
                />
            </Form.Item>
        </Section>
    );
};
