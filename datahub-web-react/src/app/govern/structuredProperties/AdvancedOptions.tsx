import { Icon, Input, Text } from '@components';
import { Collapse, Form, Tooltip } from 'antd';
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
            defaultActiveKey={[]}
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
                forceRender
            >
                <Tooltip
                    title={
                        isEditMode && 'Changing the qualified name is disabled once a structured property is created'
                    }
                    showArrow={false}
                >
                    <Form.Item name="qualifiedName">
                        <Input label="Qualified Name" placeholder="Optional - Qualified Name" isDisabled={isEditMode} />
                    </Form.Item>
                </Tooltip>
            </Collapse.Panel>
        </StyledCollapse>
    );
};

export default AdvancedOptions;
