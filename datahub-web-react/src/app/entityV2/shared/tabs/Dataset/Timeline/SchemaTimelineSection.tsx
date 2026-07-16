import { Typography } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { VerticalTimeline, VerticalTimelineElement } from 'react-vertical-timeline-component';
import 'react-vertical-timeline-component/style.min.css';
import styled, { useTheme } from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';

import { useGetTimelineQuery } from '@graphql/timeline.generated';
import { ChangeCategoryType, ChangeOperationType } from '@types';

import TimelineIcon from '@images/timeline-icon.svg?react';

const PLACEHOLDER_SUBTITLE = 'subtitle';
const PLACEHOLDER_DESC = 'description';

const TIMELINE_LAYOUT = '1-column-left';

const CONTENT_STYLE: React.CSSProperties = {
    boxShadow: 'none',
    padding: 0,
};

const ICON_STYLE: React.CSSProperties = {
    height: '10px',
    width: '10px',
    boxShadow: 'none',
    position: 'absolute',
    top: '30%',
    left: 0,
};

const TimeLine = styled(VerticalTimeline)`
    svg {
        height: 10px;
        width: 10px;
        left: 150%;
        top: 0;
        margin-left: 0;
    }
`;

const DateText = styled(Typography.Text)`
    font-size: 10px;
    font-style: normal;
    font-weight: 500;
    line-height: 16px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const Event = styled(Typography.Text)`
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 12px;
    font-weight: 500;
    letter-spacing: -0.12px;
`;

export const SchemaTimelineSection = () => {
    const { t } = useTranslation('entity.profile.timeline');
    const theme = useTheme();
    const { urn } = useEntityData();

    const getChangeTitle = (operation: ChangeOperationType | null | undefined, fieldPath: string): string => {
        switch (operation) {
            case ChangeOperationType.Add:
                return t('schemaTimeline.changeAdded', { fieldPath });
            case ChangeOperationType.Modify:
                return t('schemaTimeline.changeModified', { fieldPath });
            case ChangeOperationType.Remove:
                return t('schemaTimeline.changeRemoved', { fieldPath });
            default:
                return '';
        }
    };

    const timelineResult = useGetTimelineQuery({
        // also pass in the changeCategories
        variables: { input: { urn, changeCategories: [ChangeCategoryType.TechnicalSchema] } },
    });

    const transactions = timelineResult.data?.getTimeline?.changeTransactions;
    const timelineHistory: any[] = [];

    transactions?.forEach((transaction) => {
        const time = new Date(transaction.timestampMillis).toDateString();
        transaction.changes?.forEach((change) => {
            const fieldPath = change.parameters?.find((parameter) => parameter.key === 'fieldPath')?.value ?? '';
            const entry = {
                // operation: change.operation,
                // description: change.description,
                title: getChangeTitle(change.operation, fieldPath),
                subtitle: PLACEHOLDER_SUBTITLE,
                desc: PLACEHOLDER_DESC,
                time,
            };
            timelineHistory.push(entry);
        });
    });

    return (
        <TimeLine layout={TIMELINE_LAYOUT} lineColor={theme.colors.border} animate={false}>
            {timelineHistory.map((entry) => {
                return (
                    <VerticalTimelineElement
                        key={entry.title}
                        contentStyle={CONTENT_STYLE}
                        date={entry.date}
                        icon={<TimelineIcon />}
                        iconStyle={ICON_STYLE}
                    >
                        {entry.title && (
                            <>
                                <DateText> {entry.time}</DateText>
                                <div>
                                    <Event>{entry.title} </Event>
                                </div>
                            </>
                        )}
                    </VerticalTimelineElement>
                );
            })}
        </TimeLine>
    );
};
