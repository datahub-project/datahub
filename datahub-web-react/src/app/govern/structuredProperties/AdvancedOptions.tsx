import { Icon, Input, Text } from '@components';
import { Collapse, Form } from 'antd';
import React from 'react';
import { CollapseHeader, StyledCollapse } from './styledComponents';

interface Props {
    isEditMode: boolean;
}

const AdvancedOptions = ({ isEditMode }: Props) => {
    return (
        <StyledCollapse
            ghost
            expandIcon={({ isActive }) => (
                <Icon icon="ChevronRight" color="gray" size="4xl" rotate={isActive ? '90' : '0'} />
            )}
            expandIconPosition="end"
            defaultActiveKey={1}
        >
            <Collapse.Panel
                key={1}
                header={
                    <CollapseHeader>
                        <Text weight="bold" color="gray">
                            Advanced Options
                        </Text>
                    </CollapseHeader>
                }
            >
                <Form.Item name="qualifiedName">
                    <Input label="Namespace" placeholder="Optional - Namespace" isDisabled={isEditMode} />
                </Form.Item>
                <Form.Item name="id">
                    <Input label="ID" placeholder="Optional - Unique ID" isDisabled={isEditMode} />
                </Form.Item>
            </Collapse.Panel>
        </StyledCollapse>
    );
};

export default AdvancedOptions;
