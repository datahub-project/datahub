import React from 'react';
import Typography from 'antd/lib/typography';
import styled from 'styled-components';
import Editor from '@monaco-editor/react';
import { Form, Input } from 'antd';
import { ANTD_GRAY } from '../../../../../../../constants';

const Section = styled.div`
    margin: 16px 0 24px;
`;

const StyledFormItem = styled(Form.Item)`
    border: 1px solid ${ANTD_GRAY[5]};
    border-radius: 1px;
    background-color: ${ANTD_GRAY[2]};
`;

const QUERY_EDITOR_HEIGHT = '240px';

const QUERY_EDITOR_OPTIONS = {
    minimap: { enabled: false },
    scrollbar: {
        vertical: 'hidden',
        horizontal: 'hidden',
    },
} as any;

type Props = {
    value?: string | null;
    onChange: (newValue: string) => void;
    disabled?: boolean;
};

export const SqlQueryBuilder = ({ value, onChange, disabled }: Props) => {
    const updateQuery = (query) => {
        onChange(query);
    };

    // Render a disabled input if the editor is in read-only mode
    if (disabled) {
        return (
            <Section>
                <Typography.Title level={5}>Query</Typography.Title>
                <Input.TextArea value={value || ''} rows={10} disabled />
            </Section>
        );
    }

    return (
        <Section>
            <Typography.Title level={5}>SQL Query</Typography.Title>
            <Typography.Paragraph type="secondary">
                Define a query which returns <b>one row</b> containing <b>one column</b> of numeric type. The column
                value will be used to determine whether this assertion should pass or fail.
            </Typography.Paragraph>
            <StyledFormItem name="query" rules={[{ required: true, message: 'Required' }]}>
                <Editor
                    options={QUERY_EDITOR_OPTIONS}
                    height={QUERY_EDITOR_HEIGHT}
                    defaultLanguage="sql"
                    value={value || ''}
                    onChange={updateQuery}
                    className="query-builder-editor-input"
                />
            </StyledFormItem>
        </Section>
    );
};
