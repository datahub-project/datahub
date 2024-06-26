import React from 'react';
import styled from 'styled-components';
import { Typography, Select, Form } from 'antd';
import {
    AssertionValueChangeType,
    IncrementingSegmentSpec,
    Maybe,
    VolumeAssertionInfo,
    VolumeAssertionType,
} from '../../../../../../../../../../types.generated';
import {
    VolumeTypeOptionEnum,
    getDefaultVolumeParameters,
    getSelectedVolumeTypeOption,
    getVolumeTypeCategory,
    getVolumeTypeOption,
    getVolumeTypeOptions,
} from './utils';
import { getPropertyFromVolumeType } from '../../../../utils';

const Container = styled.div`
    margin: 16px 0 24px;
`;

const StyledSelect = styled(Select)`
    && {
        width: 300px;
    }
`;

type Props = {
    volumeInfo?: VolumeAssertionInfo;
    segment?: Maybe<IncrementingSegmentSpec>;
    onChange: (newParams: Partial<VolumeAssertionInfo>) => void;
    disabled?: boolean;
};

export const VolumeTypeBuilder = ({ volumeInfo, onChange, segment, disabled }: Props) => {
    const options = getVolumeTypeOptions();
    const selectedType = getSelectedVolumeTypeOption(volumeInfo);

    const updateVolumeType = (newValue: VolumeTypeOptionEnum) => {
        const option = getVolumeTypeOption(newValue);
        const category = getVolumeTypeCategory(option.category);
        const hasSegment = !!segment?.field;
        const volumeType = category.getType(hasSegment);
        const propertyName = getPropertyFromVolumeType(volumeType);
        const defaultParameters = getDefaultVolumeParameters(option.operator);
        const defaultChangeType = [
            VolumeAssertionType.RowCountChange,
            VolumeAssertionType.IncrementingSegmentRowCountChange,
        ].includes(volumeType)
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
            <Typography.Title level={5}>Pass when table row count</Typography.Title>
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
