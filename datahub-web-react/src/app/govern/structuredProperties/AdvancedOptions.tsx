/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Icon, Input, Text, Tooltip } from '@components';
import { Collapse, Form } from 'antd';
import React from 'react';

import {
    CollapseHeader,
    FlexContainer,
    InputLabel,
    StyledCollapse,
} from '@app/govern/structuredProperties/styledComponents';

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
                    <Form.Item
                        name="qualifiedName"
                        rules={[
                            {
                                pattern: /^[^\s]*$/,
                                whitespace: true,
                                message: 'Qualified name cannot contain spaces',
                            },
                        ]}
                    >
                        <Input label="" placeholder="Optional - Qualified Name" isDisabled={isEditMode} />
                    </Form.Item>
                </Tooltip>
            </Collapse.Panel>
        </StyledCollapse>
    );
};

export default AdvancedOptions;
