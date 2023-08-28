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
    getPropertyFromVolumeType,
    getVolumeTypeCategory,
    getVolumeTypeOption,
    getVolumeTypeOptions,
} from './utils';

const Container = styled.div`
    margin: 16px 0 24px;
`;

const StyledSelect = styled(Select)`
    && {
        width: 300px;
    }
`;

type Props = {
    segment?: Maybe<IncrementingSegmentSpec>;
    onChange: (newParams: Partial<VolumeAssertionInfo>) => void;
};

export const VolumeTypeBuilder = ({ onChange, segment }: Props) => {
    const options = getVolumeTypeOptions();

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
            <Typography.Title level={5}>Condition Type</Typography.Title>
            <Typography.Paragraph type="secondary">
                Select the scenario in which this assertion should fail
            </Typography.Paragraph>
            <Form.Item name="volume-type" rules={[{ required: true, message: 'Please select an option' }]}>
                <StyledSelect
                    placeholder="Select condition type"
                    onChange={(option) => updateVolumeType(option as VolumeTypeOptionEnum)}
                    options={options}
                />
            </Form.Item>
        </Container>
    );
};
