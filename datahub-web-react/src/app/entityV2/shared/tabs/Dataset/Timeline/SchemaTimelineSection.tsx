import { Typography } from 'antd';
import React from 'react';
import { VerticalTimeline, VerticalTimelineElement } from 'react-vertical-timeline-component';
import 'react-vertical-timeline-component/style.min.css';
import styled, { useTheme } from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';

import { useGetTimelineQuery } from '@graphql/timeline.generated';
import { ChangeCategoryType } from '@types';

import TimelineIcon from '@images/timeline-icon.svg?react';

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
    const theme = useTheme();
    const { urn } = useEntityData();

    const timelineResult = useGetTimelineQuery({
        // also pass in the changeCategories
        variables: { input: { urn, changeCategories: [ChangeCategoryType.TechnicalSchema] } },
    });

    const transactions = timelineResult.data?.getTimeline?.changeTransactions;
    const timelineHistory: any[] = [];

    transactions?.forEach((transaction) => {
        const time = new Date(transaction.timestampMillis).toDateString();
        transaction.changes?.forEach((change) => {
            const entry = {
                // operation: change.operation,
                // description: change.description,
                title: `${
                    change.parameters?.find((parameter) => parameter.key === 'fieldPath')?.value
                } was ${change.operation?.toLowerCase()}ed.`,
                subtitle: 'subtitle',
                desc: 'description',
                time,
            };
            timelineHistory.push(entry);
        });
    });

    return (
        <TimeLine layout="1-column-left" lineColor={theme.colors.border} animate={false}>
            {timelineHistory.map((t) => {
                return (
                    <VerticalTimelineElement
                        key={t.title}
                        contentStyle={{
                            boxShadow: 'none',
                            padding: 0,
                        }}
                        date={t.date}
                        icon={<TimelineIcon />}
                        iconStyle={{
                            height: '10px',
                            width: '10px',
                            boxShadow: 'none',
                            position: 'absolute',
                            top: '30%',
                            left: 0,
                        }}
                    >
                        {t.title && (
                            <>
                                <DateText> {t.time}</DateText>
                                <div>
                                    <Event>{t.title} </Event>
                                </div>
                            </>
                        )}
                    </VerticalTimelineElement>
                );
            })}
        </TimeLine>
    );
};
