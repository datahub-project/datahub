import { Collapse, Form, Input, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { Editor as MarkdownEditor } from '../../shared/tabs/Documentation/components/editor/Editor';
import { ANTD_GRAY } from '../../shared/constants';
import { DataProductBuilderState } from './types';
import { validateCustomUrnId } from '../../../shared/textUtil';

const StyledEditor = styled(MarkdownEditor)`
    border: 1px solid ${ANTD_GRAY[4]};
`;

const FormItem = styled(Form.Item)`
    .ant-form-item-label {
        padding-bottom: 2px;
    }
`;

const FormItemWithMargin = styled(FormItem)`
    margin-bottom: 16px;
`;

const FormItemNoMargin = styled(FormItem)`
    margin-bottom: 0;
`;

const AdvancedLabel = styled(Typography.Text)`
    color: #373d44;
`;

type Props = {
    builderState: DataProductBuilderState;
    updateBuilderState: (newState: DataProductBuilderState) => void;
};

export default function DataProductBuilderForm({ builderState, updateBuilderState }: Props) {
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

    function updateDataProductId(id: string) {
        updateBuilderState({
            ...builderState,
            id,
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
            <Collapse ghost>
                    <Collapse.Panel header={<AdvancedLabel>Advanced Options</AdvancedLabel>} key="1">
                        <FormItemWithMargin
                            label={<Typography.Text strong>Data Product Id</Typography.Text>}
                            help="By default, a random UUID will be generated to uniquely identify this data product. If
                                you'd like to provide a custom id instead to more easily keep track of this data product,
                                you may provide it here. Be careful, you cannot easily change the data product id after
                                creation."
                        >
                            <FormItemNoMargin
                                rules={[
                                    () => ({
                                        validator(_, value) {
                                            if (value && validateCustomUrnId(value)) {
                                                return Promise.resolve();
                                            }
                                            return Promise.reject(new Error('Please enter a valid Data product id'));
                                        },
                                    }),
                                ]}
                            >
                                <Input 
                                    data-testid="data-product-id" 
                                    placeholder="engineering" 
                                    value={builderState.id}
                                    onChange={(e) => updateDataProductId(e.target.value)}
                                />
                            </FormItemNoMargin>
                        </FormItemWithMargin>
                    </Collapse.Panel>
                </Collapse>
        </Form>
    );
}
