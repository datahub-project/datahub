import { Input, Modal } from '@components';
import dayjs from 'dayjs';
import React, { forwardRef, useEffect, useState } from 'react';

import { removeNestedTypeNames } from '@app/shared/subscribe/drawer/utils';

import { AssertionAdjustmentSettings, AssertionExclusionWindow, AssertionExclusionWindowType } from '@types';

const formatDisplayName = (startTimeMillis: number, endTimeMillis: number) => {
    return `${dayjs(startTimeMillis).format('MMM D, h:mm A')} - ${dayjs(endTimeMillis).format('MMM D, h:mm A')}`;
};

type Props = {
    inferenceSettings: AssertionAdjustmentSettings;
    onUpdateAssertionMonitorSettings: (settings: AssertionAdjustmentSettings) => Promise<void>;
};

export type AddNamedExclusionWindowModalRef = {
    create: ({ startTimeMillis, endTimeMillis }: { startTimeMillis: number; endTimeMillis: number }) => void;
};

export const AddNamedExclusionWindowModal = forwardRef<AddNamedExclusionWindowModalRef, Props>(
    ({ inferenceSettings, onUpdateAssertionMonitorSettings }, ref) => {
        // -------------- State -------------- //
        const [isOpen, setIsOpen] = useState(false);

        const [range, setRange] = useState<{ startTimeMillis: number; endTimeMillis: number } | null>(null);
        const [name, setName] = useState('');
        useEffect(() => {
            if (range) {
                setName(formatDisplayName(range.startTimeMillis, range.endTimeMillis));
            }
        }, [range]);

        // -------------- Event handlers -------------- //

        const onAddExclusionWindow = async (startTimeMillis: number, endTimeMillis: number, nameInput?: string) => {
            const displayName = nameInput || formatDisplayName(startTimeMillis, endTimeMillis);
            const newExclusionWindow: AssertionExclusionWindow = {
                type: AssertionExclusionWindowType.FixedRange,
                displayName,
                fixedRange: { startTimeMillis, endTimeMillis },
            };
            const existingInferenceSettings = removeNestedTypeNames(inferenceSettings);
            await onUpdateAssertionMonitorSettings({
                ...existingInferenceSettings,
                exclusionWindows: [...(existingInferenceSettings?.exclusionWindows ?? []), newExclusionWindow],
            });
        };

        const onOk = () => {
            if (!range) {
                return;
            }
            onAddExclusionWindow(range.startTimeMillis, range.endTimeMillis, name);
            setIsOpen(false);
        };
        // -------------- Refs -------------- //
        React.useImperativeHandle(ref, () => ({
            create: ({ startTimeMillis, endTimeMillis }) => {
                setRange({ startTimeMillis, endTimeMillis });
                setIsOpen(true);
            },
        }));

        // -------------- Render -------------- //
        return (
            <Modal
                open={isOpen}
                title="Add Exclusion Window"
                onCancel={() => {
                    setIsOpen(false);
                }}
                buttons={[
                    {
                        text: 'Cancel',
                        variant: 'secondary',
                        onClick: () => {
                            setIsOpen(false);
                        },
                    },
                    {
                        text: 'Add',
                        onClick: onOk,
                        disabled: !range,
                    },
                ]}
            >
                <Input
                    label="Give this exclusion window a helpful name"
                    placeholder="Optional"
                    value={name}
                    onChange={(e) => setName(e.target.value)}
                />
            </Modal>
        );
    },
);
