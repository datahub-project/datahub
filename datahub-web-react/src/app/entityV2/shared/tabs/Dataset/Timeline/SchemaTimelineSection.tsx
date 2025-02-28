import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { VerticalTimeline, VerticalTimelineElement } from 'react-vertical-timeline-component';
import { useEntityData } from '../../../../../entity/shared/EntityContext';
import { useGetTimelineQuery } from '../../../../../../graphql/timeline.generated';
import { ChangeCategoryType } from '../../../../../../types.generated';
import 'react-vertical-timeline-component/style.min.css';
import { REDESIGN_COLORS } from '../../../constants';
import TimelineIcon from '../../../../../../images/timeline-icon.svg?react';

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
    color: ${REDESIGN_COLORS.DARK_GREY};
`;

const Event = styled(Typography.Text)`
    color: ${REDESIGN_COLORS.DARK_GREY};
    font-size: 12px;
    font-weight: 500;
    letter-spacing: -0.12px;
`;

export const SchemaTimelineSection = () => {
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
        <TimeLine layout="1-column-left" lineColor={REDESIGN_COLORS.SIDE_BAR} animate={false}>
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
