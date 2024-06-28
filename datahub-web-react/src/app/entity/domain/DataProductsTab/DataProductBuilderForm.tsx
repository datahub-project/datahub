import { Form, Input, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { Editor as MarkdownEditor } from '../../shared/tabs/Documentation/components/editor/Editor';
import { ANTD_GRAY } from '../../shared/constants';
import { DataProductBuilderFormProps } from './types';
import { DataProductAdvancedOption } from './DataProductAdvancedOption';

const StyledEditor = styled(MarkdownEditor)`
    border: 1px solid ${ANTD_GRAY[4]};
`;

export default function DataProductBuilderForm({ builderState, updateBuilderState }: DataProductBuilderFormProps) {
    function updateName(name: string) {
        updateBuilderState({
            ...builderState,
            name,
        });
    }

    function updateDescription(description: string) {
        updateBuilderState({
            ...builderState,
            description,
        });
    }

    return (
        <Form layout="vertical">
            <Form.Item
                rules={[{ min: 1, max: 500 }]}
                hasFeedback
                label={<Typography.Text strong>Name</Typography.Text>}
                required
            >
                <Input
                    autoFocus
                    value={builderState.name}
                    onChange={(e) => updateName(e.target.value)}
                    placeholder="Revenue Dashboards"
                />
            </Form.Item>
            <Form.Item label={<Typography.Text strong>Description</Typography.Text>}>
                <StyledEditor doNotFocus content={builderState.description} onChange={updateDescription} />
            </Form.Item>
            <DataProductAdvancedOption builderState={builderState} updateBuilderState={updateBuilderState} />
        </Form>
    );
}
