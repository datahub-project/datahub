import { Icon, Input, Text } from '@components';
import { StructuredPropertyFilterStatus } from '@src/types.generated';
import { Checkbox, Collapse, Form, Tooltip } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { CollapseHeader, FlexContainer, InputLabel, StyledCollapse } from './styledComponents';
import { StructuredProp } from './utils';

const CheckboxWrapper = styled.div`
    display: flex;
    align-items: center;
    gap: 10px;
    margin: 10px 0;
    color: #374066;
    p {
        color: #374066;
        font-weight: 500;
    }
`;

const StyledText = styled.div`
    display: inline-flex;
    margin-left: -4px;
`;

const StyledFormItem = styled(Form.Item)`
    // line-height: normal
    margin: 0;
`;

interface Props {
    isEditMode: boolean;
    formValues?: StructuredProp;
    handleFilterStatusChange: (showInFilters: boolean) => void;
}

const AdvancedOptions = ({ isEditMode, formValues, handleFilterStatusChange }: Props) => {
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
                <InputLabel>
                    <FlexContainer>
                        Qualified Name
                        <Tooltip
                            title="Optionally provide a dot-separated fully qualified name for this property. This name serves as an ID, and must be unique across properties"
                            showArrow={false}
                        >
                            <Icon icon="Info" color="violet" size="lg" />
                        </Tooltip>
                    </FlexContainer>
                </InputLabel>
                <Tooltip
                    title={isEditMode && 'Once a property is created, Qualified Name cannot be changed'}
                    showArrow={false}
                >
                    <Form.Item name="qualifiedName">
                        <Input label="" placeholder="Optional - Qualified Name" isDisabled={isEditMode} />
                    </Form.Item>
                </Tooltip>
                <CheckboxWrapper>
                    <StyledFormItem name="filterStatus">
                        <Checkbox
                            checked={formValues?.filterStatus === StructuredPropertyFilterStatus.Enabled}
                            onChange={(e) => handleFilterStatusChange(e.target.checked)}
                        />
                    </StyledFormItem>
                    <Text size="md">Display in filters</Text>
                    <StyledText>
                        <Tooltip title="If enabled, this property will appear in search filters">
                            <Icon icon="Info" size="lg" />
                        </Tooltip>
                    </StyledText>
                </CheckboxWrapper>
            </Collapse.Panel>
        </StyledCollapse>
    );
};

export default AdvancedOptions;
