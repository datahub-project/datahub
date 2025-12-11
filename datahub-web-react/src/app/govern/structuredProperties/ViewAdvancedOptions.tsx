/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Icon, Text } from '@components';
import { Collapse } from 'antd';
import React from 'react';

import {
    CollapseHeader,
    RowContainer,
    StyledCollapse,
    StyledLabel,
} from '@app/govern/structuredProperties/styledComponents';
import { StructuredPropertyEntity } from '@src/types.generated';

interface Props {
    propEntity: StructuredPropertyEntity;
}

const ViewAdvancedOptions = ({ propEntity }: Props) => {
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
                        <Text weight="bold" color="gray" size="lg">
                            Advanced Options
                        </Text>
                    </CollapseHeader>
                }
                forceRender
            >
                {propEntity && (
                    <RowContainer>
                        <StyledLabel>Qualified Name</StyledLabel>
                        <Text color="gray"> {propEntity?.definition?.qualifiedName}</Text>
                    </RowContainer>
                )}
            </Collapse.Panel>
        </StyledCollapse>
    );
};

export default ViewAdvancedOptions;
