import React from 'react';
import styled from 'styled-components';
import { Select, Typography } from 'antd';
import { Tooltip } from '@components';
import { ActionType } from '../../property/types/action';

const StyledSelect = styled(Select)`
    width: 200px;
    margin-right: 12px;
`;

type Props = {
    selectedActionTypeId?: string;
    actionTypes: ActionType[];
    onChangeActionType: (newActionTypeId: string) => void;
};

/**
 * A component useful in selecting a specific type of action.
 */
export const ActionTypeSelect = ({ selectedActionTypeId, actionTypes, onChangeActionType }: Props) => {
    return (
        <StyledSelect
            defaultActiveFirstOption={false}
            placeholder="Select an action type..."
            onSelect={(newVal) => onChangeActionType(newVal as string)}
            value={selectedActionTypeId?.toLowerCase()}
        >
            {actionTypes?.map((actionType) => {
                return (
                    <Select.Option value={actionType.id.toLowerCase()} key={actionType.id.toLowerCase()}>
                        <Tooltip title={actionType.description} placement="right">
                            <Typography.Text>{actionType.displayName}</Typography.Text>
                        </Tooltip>
                    </Select.Option>
                );
            })}
        </StyledSelect>
    );
};
