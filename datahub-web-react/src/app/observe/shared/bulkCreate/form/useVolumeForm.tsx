import { SimpleSelect, Text, ToggleCard } from '@components';
import { Divider, Form } from 'antd';
import React, { useEffect } from 'react';

import { AssertionActionsForm } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/AssertionActionsForm';
import { VolumeInferenceAdjuster } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/VolumeInferenceAdjuster';
import {
    VOLUME_SOURCE_TYPES,
    getDefaultVolumeSourceType,
    getVolumeSourceTypeOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/volume/utils';
import { AssertionMonitorBuilderState } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import {
    DEFAULT_ASSERTION_ACTIONS,
    DEFAULT_CRON_SCHEDULE,
    DEFAULT_INFERENCE_SETTINGS,
} from '@app/observe/shared/bulkCreate/form/BulkCreateAssertionsForm.constants';
import { VolumeFormState } from '@app/observe/shared/bulkCreate/form/types';

import { AssertionActionsInput, CronSchedule, DatasetVolumeSourceType } from '@types';

export const useVolumeForm = ({
    selectedPlatformUrn,
}: {
    selectedPlatformUrn?: string;
}): {
    component: React.ReactNode;
    state: VolumeFormState;
} => {
    // --------------------------------- State variables --------------------------------- //
    const [volumeAssertionEnabled, setVolumeAssertionEnabled] = React.useState<boolean>(false);
    const [volumeInferenceSettings, setVolumeInferenceSettings] =
        React.useState<AssertionMonitorBuilderState['inferenceSettings']>(DEFAULT_INFERENCE_SETTINGS);

    const [volumeSchedule, setVolumeSchedule] = React.useState<CronSchedule>({
        cron: DEFAULT_CRON_SCHEDULE.cron,
        timezone: DEFAULT_CRON_SCHEDULE.timezone,
    });

    const [volumeActions, setVolumeActions] = React.useState<AssertionActionsInput>(DEFAULT_ASSERTION_ACTIONS);

    const volumeSourceOptions = selectedPlatformUrn ? getVolumeSourceTypeOptions(selectedPlatformUrn, true, false) : [];
    const defaultVolumeSource = selectedPlatformUrn ? getDefaultVolumeSourceType(selectedPlatformUrn, true) : undefined;
    const [selectedVolumeSource, setSelectedVolumeSource] = React.useState<DatasetVolumeSourceType | undefined>(
        defaultVolumeSource,
    );
    useEffect(() => {
        if (defaultVolumeSource && !selectedVolumeSource) {
            setSelectedVolumeSource(defaultVolumeSource);
        }
    }, [defaultVolumeSource, selectedVolumeSource]);

    // --------------------------------- Render UI --------------------------------- //
    const component = (
        <ToggleCard
            title="Volume"
            value={volumeAssertionEnabled}
            onToggle={() => {
                setVolumeAssertionEnabled((enabled) => !enabled);
            }}
        >
            {/* --------------------------------- Title --------------------------------- */}
            <Text size="md" color="gray" colorLevel={1700}>
                Find out when there are row count anomalies within your datasets.
            </Text>

            {volumeAssertionEnabled && [
                // --------------------------------- Volume Inference Settings --------------------------------- //
                // We have to wrap this in a Form since the nested components depend on antd Form context.
                <Form>
                    <VolumeInferenceAdjuster
                        state={{ inferenceSettings: volumeInferenceSettings, schedule: volumeSchedule }}
                        updateState={(newState) => {
                            setVolumeInferenceSettings(newState.inferenceSettings);
                            setVolumeSchedule(newState.schedule || volumeSchedule);
                        }}
                        collapsable
                    />
                </Form>,
                // --------------------------------- Volume Detection Mechanism --------------------------------- //
                <SimpleSelect
                    label="Volume Detection Mechanism"
                    options={volumeSourceOptions.map((source) => ({
                        label: VOLUME_SOURCE_TYPES[source].label,
                        value: source,
                        description: VOLUME_SOURCE_TYPES[source].description,
                    }))}
                    values={selectedVolumeSource ? [selectedVolumeSource] : []}
                    isMultiSelect={false}
                    showClear={false}
                    position="start"
                    width="fit-content"
                    descriptionMaxWidth={600}
                    onUpdate={(values) => {
                        if (values.length > 0) {
                            setSelectedVolumeSource(values[0]?.toUpperCase() as DatasetVolumeSourceType);
                        }
                    }}
                />,
                <Divider />,
                // --------------------------------- Assertion Actions --------------------------------- //
                <AssertionActionsForm
                    state={volumeActions}
                    updateState={(newState) =>
                        setVolumeActions({
                            onFailure: newState.onFailure ?? [],
                            onSuccess: newState.onSuccess ?? [],
                        })
                    }
                />,
            ]}
        </ToggleCard>
    );

    return {
        component,
        state: {
            volumeAssertionEnabled,
            volumeInferenceSettings,
            volumeSchedule,
            volumeActions,
            volumeSourceType: selectedVolumeSource ?? DatasetVolumeSourceType.DatahubDatasetProfile,
        },
    };
};
