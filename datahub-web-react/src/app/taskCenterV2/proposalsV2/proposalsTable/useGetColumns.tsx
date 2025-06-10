import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import ProposalsAssignee from '@app/taskCenterV2/proposalsV2/ProposalsAssignee';
import ActionsColumn from '@app/taskCenterV2/proposalsV2/proposalsTable/ActionsColumn';
import { colors } from '@src/alchemy-components';
import { AlignmentOptions } from '@src/alchemy-components/theme/config';
import { ActionRequestListItem } from '@src/app/actionrequestV2/item/ActionRequestListItem';
import { toLocalDateString } from '@src/app/shared/time/timeUtils';

import { ActionRequest } from '@types';

const OverflowText = styled(Typography.Text)`
    color: ${colors.gray[1700]};
    overflow: hidden;
`;

interface Props {
    onActionRequestUpdate: (completedUrns: string[]) => void;
    showPendingView?: boolean;
    showAssignee?: boolean;
}

export const useGetColumns = ({ onActionRequestUpdate, showPendingView, showAssignee = false }: Props) => {
    const columns = [
        {
            title: 'Content',
            key: 'content',
            render: (record: ActionRequest) => {
                return <ActionRequestListItem actionRequest={record} />;
            },
            minWidth: '650px',
        },
        ...(showAssignee
            ? [
                  {
                      title: 'To',
                      key: 'To',
                      render: (record: ActionRequest) => {
                          return <ProposalsAssignee assignees={record?.assignees || []} />;
                      },
                      width: '10%',
                  },
              ]
            : []),
        {
            title: 'Date',
            key: 'date',
            render: (record: ActionRequest) => {
                const createdTime = record.created.time;
                return <>{toLocalDateString(createdTime)}</>;
            },
            width: '10%',
            sorter: (sourceA, sourceB) => {
                const timeA = sourceA.created.time || Number.MAX_SAFE_INTEGER;
                const timeB = sourceB.created.time || Number.MAX_SAFE_INTEGER;

                return timeA - timeB;
            },
        },
        {
            title: 'Note',
            key: 'note',
            render: (record: ActionRequest) => {
                return (
                    <OverflowText
                        ellipsis={{
                            tooltip: {
                                title: record.description,
                                showArrow: false,
                            },
                        }}
                    >
                        {record.description}
                    </OverflowText>
                );
            },
            width: '200px',
        },
        {
            title: '',
            key: 'actions',
            render: (record: ActionRequest) => {
                return (
                    <ActionsColumn
                        actionRequest={record}
                        onUpdate={onActionRequestUpdate}
                        showPendingView={showPendingView}
                    />
                );
            },
            width: '10%',
            alignment: 'right' as AlignmentOptions,
        },
    ];

    return columns;
};
