import React from 'react';
import styled from 'styled-components';
import { Dropdown, Menu, Typography } from 'antd';
import { LogicalOperatorType } from './types';
import { ANTD_GRAY } from '../../../../../entity/shared/constants';

const DropdownWrapper = styled.div<{
    disabled: boolean;
}>`
    cursor: ${(props) => (props.disabled ? 'normal' : 'pointer')};
    color: ${(props) => (props.disabled ? ANTD_GRAY[6] : ANTD_GRAY[8])};
    display: flex;
    margin-left: 4px;
    margin-right: 12px;
    :hover {
        text-decoration: underline;
    }
`;

const OptionDescription = styled(Typography.Paragraph)`
    && {
        margin: 0px;
        padding: 0px;
    }
`;

type Props = {
    disabled?: boolean;
    onAddPropertyPredicate: () => void;
    onAddLogicalPredicate: (operator: LogicalOperatorType) => void;
    options?: {
        predicateDisplayName?: string;
    };
};

export const AddPredicateButton = ({
    disabled = false,
    options = { predicateDisplayName: 'predicate' },
    onAddPropertyPredicate,
    onAddLogicalPredicate,
}: Props) => {
    return (
        <Dropdown
            disabled={disabled}
            trigger={['click']}
            overlay={
                <Menu>
                    <Menu.Item onClick={onAddPropertyPredicate}>
                        <Typography.Text strong>Property</Typography.Text> {options?.predicateDisplayName}
                        <OptionDescription type="secondary">
                            Match a particular property or attribute of a data asset
                        </OptionDescription>
                    </Menu.Item>
                    <Menu.Item onClick={() => onAddLogicalPredicate(LogicalOperatorType.AND)}>
                        <Typography.Text strong>AND</Typography.Text> {options?.predicateDisplayName}
                        <OptionDescription type="secondary">Add a nested AND block</OptionDescription>
                    </Menu.Item>
                    <Menu.Item onClick={() => onAddLogicalPredicate(LogicalOperatorType.OR)}>
                        <Typography.Text strong>OR</Typography.Text> {options?.predicateDisplayName}
                        <OptionDescription type="secondary">Add a nested OR block</OptionDescription>
                    </Menu.Item>
                    <Menu.Item onClick={() => onAddLogicalPredicate(LogicalOperatorType.NOT)}>
                        <Typography.Text strong>NOT</Typography.Text> {options?.predicateDisplayName}
                        <OptionDescription type="secondary">Add a nested NOT block</OptionDescription>
                    </Menu.Item>
                </Menu>
            }
        >
            <DropdownWrapper disabled={disabled}>
                <b>+ Add {options?.predicateDisplayName}</b>
            </DropdownWrapper>
        </Dropdown>
    );
};
