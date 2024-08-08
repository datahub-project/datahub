import React from 'react';
import { Collapse, Form, Input, Typography } from 'antd';
import styled from 'styled-components';
import { validateCustomUrnId } from '../../../shared/textUtil';
import { DataProductBuilderFormProps } from './types';

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

export function DataProductAdvancedOption({ builderState, updateBuilderState }: DataProductBuilderFormProps) {
    function updateDataProductId(id: string) {
        updateBuilderState({
            ...builderState,
            id,
        });
    }

    return (
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
    );
}
