import { Editor } from '@components';
import { Form, Input, Typography } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { DataProductBuilderState } from '@app/entityV2/domain/DataProductsTab/types';

const StyledEditor = styled(Editor)`
    border: 1px solid ${(props) => props.theme.colors.bgHover};
`;

type Props = {
    builderState: DataProductBuilderState;
    updateBuilderState: (newState: DataProductBuilderState) => void;
};

export default function DataProductBuilderForm({ builderState, updateBuilderState }: Props) {
    const { t: tl } = useTranslation('common.labels');
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
                label={<Typography.Text strong>{tl('name')}</Typography.Text>}
                data-testid="name-input"
                required
            >
                {/* eslint-disable i18next/no-literal-string -- (untranslated-text) example-value placeholder; illustrative sample text intentionally kept in EN */}
                <Input
                    autoFocus
                    value={builderState.name}
                    onChange={(e) => updateName(e.target.value)}
                    placeholder="Revenue Dashboards"
                />
                {/* eslint-enable i18next/no-literal-string */}
            </Form.Item>
            <Form.Item label={<Typography.Text strong>{tl('description')}</Typography.Text>}>
                <StyledEditor doNotFocus content={builderState.description} onChange={updateDescription} />
            </Form.Item>
        </Form>
    );
}
