import { toLocalDateString } from '@src/app/shared/time/timeUtils';
import React from 'react';
import { AlignmentOptions } from '@src/alchemy-components/theme/config';
import { ActionRequestListItem } from '@src/app/actionrequestV2/item/ActionRequestListItem';
import { Typography } from 'antd';
import styled from 'styled-components';
import { colors } from '@src/alchemy-components';
import ActionsColumn from './ActionsColumn';

const OverflowText = styled(Typography.Text)`
    color: ${colors.gray[1700]};
    overflow: hidden;
`;

interface Props {
    onActionRequestUpdate: () => void;
    showPendingView?: boolean;
}

export const useGetColumns = ({ onActionRequestUpdate, showPendingView }: Props) => {
    const columns = [
        {
            title: 'Content',
            key: 'content',
            render: (record) => {
                return <ActionRequestListItem actionRequest={record} />;
            },
            minWidth: '650px',
        },
        {
            title: 'Date',
            key: 'date',
            render: (record) => {
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
            render: (record) => {
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
            render: (record) => {
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
