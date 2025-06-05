import { Form, Select, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import {
    VolumeTypeOptionEnum,
    getDefaultVolumeParameters,
    getSelectedVolumeTypeOption,
    getVolumeTypeCategory,
    getVolumeTypeOption,
    getVolumeTypeOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/volume/utils';
import {
    VolumeAssertionBuilderState,
    VolumeAssertionBuilderType,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import { getPropertyFromVolumeType } from '@app/entityV2/shared/tabs/Dataset/Validations/utils';
import { useAppConfig } from '@src/app/useAppConfig';

import { AssertionValueChangeType, IncrementingSegmentSpec, Maybe, VolumeAssertionType } from '@types';

const Container = styled.div`
    margin: 16px 0 24px;
`;

const StyledSelect = styled(Select)`
    && {
        width: 300px;
    }
`;

type Props = {
    volumeInfo?: VolumeAssertionBuilderState;
    segment?: Maybe<IncrementingSegmentSpec>;
    onChange: (newParams: VolumeAssertionBuilderState) => void;
    disabled?: boolean;
    isEditMode?: boolean;
};

export const VolumeTypeBuilder = ({ volumeInfo, onChange, segment, disabled, isEditMode }: Props) => {
    const { onlineSmartAssertionsEnabled } = useAppConfig().config.featureFlags;

    const options = getVolumeTypeOptions({ disableAiInferred: isEditMode || !onlineSmartAssertionsEnabled });
    const selectedType = getSelectedVolumeTypeOption(volumeInfo);

    const updateVolumeType = (newValue: VolumeTypeOptionEnum) => {
        const option = getVolumeTypeOption(newValue);
        const category = getVolumeTypeCategory(option.category);
        const hasSegment = !!segment?.field;
        const volumeType = category.getType(hasSegment);

        const propertyName = getPropertyFromVolumeType(volumeType);
        const defaultParameters = getDefaultVolumeParameters(option.operator);
        const absoluteChangeTypes: VolumeAssertionBuilderType[] = [
            VolumeAssertionType.RowCountChange,
            VolumeAssertionType.IncrementingSegmentRowCountChange,
        ];
        const defaultChangeType = absoluteChangeTypes.includes(volumeType)
            ? { type: AssertionValueChangeType.Absolute }
            : {};

        onChange({
            type: volumeType,
            parameters: defaultParameters,
            [propertyName]: {
                ...defaultChangeType,
                operator: option.operator,
            },
        } as any);
    };

    return (
        <Container>
            <Typography.Title level={5}>Table row count should be...</Typography.Title>
            <Form.Item
                initialValue={selectedType}
                name="volume-type"
                rules={[{ required: true, message: 'Please select an option' }]}
            >
                <StyledSelect
                    value={selectedType}
                    placeholder="Select volume condition"
                    onChange={(option) => updateVolumeType(option as VolumeTypeOptionEnum)}
                    options={options}
                    disabled={disabled}
                />
            </Form.Item>
        </Container>
    );
};
