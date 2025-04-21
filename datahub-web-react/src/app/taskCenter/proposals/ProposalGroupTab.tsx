import { CheckSquareOutlined, ClockCircleOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React, { useState } from 'react';

import TabToolbar from '@app/entity/shared/components/styled/TabToolbar';
import { ProposalList } from '@app/taskCenter/proposals/ProposalList';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';

import { ActionRequestAssignee, ActionRequestStatus } from '@types';

type Props = {
    // The assignee associated with the action request groups,
    // so they can be viewed individually for each group or user.
    assignee?: ActionRequestAssignee;
};

export const ProposalGroupTab = ({ assignee }: Props) => {
    const { theme } = useCustomTheme();
    /**
     * Determines which view should be visible: pending or completed requests.
     */
    const [viewType, setViewType] = useState<string>(ActionRequestStatus.Pending);

    const pendingColor = viewType === ActionRequestStatus.Pending ? getColor('primary', 500, theme) : undefined;
    const completedColor = viewType === ActionRequestStatus.Completed ? getColor('primary', 500, theme) : undefined;

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
            <ProposalList
                status={(viewType as ActionRequestStatus) || ActionRequestStatus.Pending}
                assignee={assignee}
            />
        </>
    );
};
