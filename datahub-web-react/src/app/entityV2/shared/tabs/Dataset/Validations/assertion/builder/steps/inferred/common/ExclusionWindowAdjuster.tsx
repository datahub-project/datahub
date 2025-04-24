import { DatePicker, Dropdown, Form, List } from 'antd';
import dayjs from 'dayjs';
import React, { useState } from 'react';

import { AssertionFormTitleAndTooltip } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/AssertionFormTitleAndTooltip';
import { AssertionMonitorBuilderExclusionWindow } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import { Pill } from '@src/alchemy-components';
import { AssertionExclusionWindowType } from '@src/types.generated';

type Props = {
    exclusionWindows: AssertionMonitorBuilderExclusionWindow;
    disabled?: boolean;
    onChange: (value: AssertionMonitorBuilderExclusionWindow) => void;
};

export const ExclusionWindowAdjuster = (props: Props) => {
    const { exclusionWindows, disabled, onChange } = props;
    const [isDropdownOpen, setIsDropdownOpen] = useState(false);

    const onRemoveExclusionWindow = (index: number) => {
        const newExclusionWindows = [...exclusionWindows];
        newExclusionWindows.splice(index, 1);
        onChange(newExclusionWindows);
    };

    const onAddExclusionWindow = (startTime: number, endTime: number) => {
        const newExclusionWindows = [
            ...exclusionWindows,
            {
                type: AssertionExclusionWindowType.FixedRange,
                displayName: `${dayjs(startTime).format('MMM D, h:mm A')} - ${dayjs(endTime).format('MMM D, h:mm A')}`,
                fixedRange: {
                    startTimeMillis: startTime,
                    endTimeMillis: endTime,
                },
            },
        ];
        onChange(newExclusionWindows);
        setIsDropdownOpen(false);
    };

    const dropdownContent = (
        <div
            style={{
                padding: '8px',
                background: 'white',
                borderRadius: '4px',
                boxShadow: '0 2px 8px rgba(0,0,0,0.15)',
            }}
        >
            <DatePicker.RangePicker
                showTime
                format="MMM D, h:mm A"
                onChange={(dates) => {
                    if (dates && dates[0] && dates[1]) {
                        const [start, end] = dates;
                        onAddExclusionWindow(start.valueOf(), end.valueOf());
                    }
                }}
            />
        </div>
    );

    const exclusionWindowsWithAddButton: (AssertionMonitorBuilderExclusionWindow[0] | { type: 'ADD_BUTTON' })[] =
        exclusionWindows.concat({
            type: 'ADD_BUTTON' as any,
        });
    return (
        <Form.Item
            label={
                <AssertionFormTitleAndTooltip
                    formTitle="Exclusion Windows"
                    tooltipTitle="Exclusion Windows"
                    tooltipDescription="Set time windows to exclude from training data. This can be useful to exclude known maintenance windows or other periods of downtime, seasonal spikes e.g. holidays or any other windows that are not representative of normal data trends."
                />
            }
            style={{ marginBottom: 16 }}
            labelCol={{ span: 24 }}
            wrapperCol={{ span: 24 }}
        >
            {/* Preview list of exclusion windows in little pills with an add button at the end of the list */}
            <List
                style={{ display: 'flex', flexDirection: 'row', gap: 8 }}
                dataSource={exclusionWindowsWithAddButton}
                renderItem={(item, index) => (
                    <List.Item style={{ padding: 2, border: 'none' }}>
                        {item.type === 'ADD_BUTTON' ? (
                            <Dropdown
                                open={isDropdownOpen}
                                onOpenChange={setIsDropdownOpen}
                                dropdownRender={() => dropdownContent}
                                trigger={['click']}
                            >
                                <Pill
                                    label="Add"
                                    leftIcon="Add"
                                    clickable={!disabled}
                                    onPillClick={() => setIsDropdownOpen(true)}
                                />
                            </Dropdown>
                        ) : (
                            <Pill
                                label={item.displayName || item.type.toLowerCase().replace('_', ' ')}
                                rightIcon="Delete"
                                onClickRightIcon={() => onRemoveExclusionWindow(index)}
                                clickable={!disabled}
                            />
                        )}
                    </List.Item>
                )}
            />
        </Form.Item>
    );
};
