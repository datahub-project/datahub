import { CheckSquareOutlined, ClockCircleOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React, { useState } from 'react';
import { useTheme } from 'styled-components';
import { ActionRequestAssignee, ActionRequestStatus } from '../../types.generated';
import TabToolbar from '../entity/shared/components/styled/TabToolbar';
import { ActionRequestsList } from './ActionRequestsList';

type Props = {
    // The assignee associated with the action request groups,
    // so they can be viewed individually for each group or user.
    assignee?: ActionRequestAssignee;
};

export const ActionRequestsGroupTab = ({ assignee }: Props) => {
    const theme = useTheme();
    /**
     * Determines which view should be visible: pending or completed requests.
     */
    const [viewType, setViewType] = useState<string>(ActionRequestStatus.Pending);

    const pendingColor = viewType === ActionRequestStatus.Pending ? theme.styles['primary-color'] : undefined;
    const completedColor = viewType === ActionRequestStatus.Completed ? theme.styles['primary-color'] : undefined;

    return (
        <>
            <TabToolbar>
                <div>
                    <Button
                        type="text"
                        style={{ color: pendingColor }}
                        onClick={() => setViewType(ActionRequestStatus.Pending)}
                    >
                        <ClockCircleOutlined />
                        Pending
                    </Button>
                    <Button
                        type="text"
                        style={{ color: completedColor }}
                        onClick={() => setViewType(ActionRequestStatus.Completed)}
                    >
                        <CheckSquareOutlined />
                        Completed
                    </Button>
                </div>
            </TabToolbar>
            <ActionRequestsList
                status={(viewType as ActionRequestStatus) || ActionRequestStatus.Pending}
                assignee={assignee}
            />
        </>
    );
};
