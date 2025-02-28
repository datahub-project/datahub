import { Icon, Text } from '@components';
import { StructuredPropertyEntity } from '@src/types.generated';
import { Collapse } from 'antd';
import React from 'react';
import { CollapseHeader, RowContainer, StyledCollapse, StyledLabel } from './styledComponents';

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
