import { Select, Typography } from 'antd';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

type Props = {
    updateCron: (newCron: string) => void;
};

const SelectTemplateHeader = styled(Typography.Title)`
    && {
        margin-bottom: 16px;
    }
`;

export const CronBuilder = ({ updateCron }: Props) => {
    const [cadence, setCadence] = useState<'daily' | 'weekly' | 'monthly'>('daily');
    const [dayOfMonth, setDayOfMonth] = useState<undefined | string>(undefined);
    const [dayOfWeek, setDayOfWeek] = useState<undefined | string>(undefined);
    const [hour, setHour] = useState<undefined | string>(undefined);
    const [minute, setMinute] = useState<undefined | string>(undefined);

    const cronToDisplay = useMemo(() => {
        let newCron;
        if (cadence === 'daily') {
            newCron = `${minute} ${hour} * * *`;
        } else if (cadence === 'weekly') {
            newCron = `${minute} ${hour} * * ${dayOfWeek}`;
        } else if (cadence === 'monthly') {
            newCron = `${minute} ${hour} ${dayOfMonth} * *`;
        }
        updateCron(newCron);
        return newCron;
    }, [cadence, dayOfMonth, dayOfWeek, hour, minute, updateCron]);

    const onChangeCadence = (newCadence: any) => {
        setCadence(newCadence);
    };

    const onChangeDayOfMonth = (newDayOfMonth) => {
        console.log(newDayOfMonth);
        setDayOfMonth(newDayOfMonth);
    };

    const onChangeDayOfWeek = (newDayOfWeek) => {
        console.log(newDayOfWeek);
        setDayOfWeek(newDayOfWeek);
    };

    const onChangeHour = (newHour) => {
        console.log(newHour);
        setHour(newHour);
    };

    const onChangeMinute = (newMinute) => {
        console.log(newMinute);
        setMinute(newMinute);
    };

    const showHourPicker = true; // daily, weekly, monthly.
    const showDayOfWeekPicker = cadence === 'weekly';
    const showDayOfMonthPicker = cadence === 'monthly';

    return (
        <>
            <SelectTemplateHeader level={5}>How often do you want to run ingestion?</SelectTemplateHeader>
            <Select onChange={onChangeCadence}>
                <Select.Option value="days">Every day</Select.Option>
                <Select.Option value="weeks">Every week</Select.Option>
                <Select.Option value="months">Every month</Select.Option>
            </Select>
            {showDayOfMonthPicker && (
                <>
                    <Typography.Text>On day</Typography.Text>
                    <Select onChange={(val) => onChangeDayOfMonth(val)}>
                        {Array(30)
                            .fill(0)
                            .map((_, i) => (
                                <Select.Option value={i + 1}>{i + 1}</Select.Option>
                            ))}
                    </Select>
                </>
            )}
            {showDayOfWeekPicker && (
                <>
                    <Typography.Text>On</Typography.Text>
                    <Select onChange={(val) => onChangeDayOfWeek(val)}>
                        <Select.Option value="0">Sunday</Select.Option>
                        <Select.Option value="1">Monday</Select.Option>
                        <Select.Option value="2">Tuesday</Select.Option>
                        <Select.Option value="3">Wednesday</Select.Option>
                        <Select.Option value="4">Thursday</Select.Option>
                        <Select.Option value="5">Friday</Select.Option>
                        <Select.Option value="6">Saturday</Select.Option>
                    </Select>
                </>
            )}
            {showHourPicker && (
                <>
                    <Typography.Text>At (UTC)</Typography.Text>
                    <span>
                        <Select onChange={(val) => onChangeHour(val)}>
                            {Array(24)
                                .fill(0)
                                .map((_, i) => (
                                    <Select.Option value={i}>{i}</Select.Option>
                                ))}
                        </Select>
                        <Typography.Text>:</Typography.Text>
                        <Select onChange={(val) => onChangeMinute(val)}>
                            {Array(60)
                                .fill(0)
                                .map((_, i) => (
                                    <Select.Option value={i}>{i}</Select.Option>
                                ))}
                        </Select>
                    </span>
                </>
            )}
            <>
                <Typography.Text>Cron Representation</Typography.Text>
                <Typography.Text>{cronToDisplay}</Typography.Text>
            </>
        </>
    );
};
