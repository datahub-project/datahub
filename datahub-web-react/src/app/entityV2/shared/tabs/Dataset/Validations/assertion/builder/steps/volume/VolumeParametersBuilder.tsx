import { Form } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { VolumeRowCountChangeBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/volume/VolumeRowCountChangeBuilder';
import { VolumeRowCountTotalBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/volume/VolumeRowCountTotalBuilder';
import {
    VolumeAssertionBuilderState,
    VolumeAssertionBuilderTypeOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import { getIsRowCountChange, getPropertyFromVolumeType } from '@app/entityV2/shared/tabs/Dataset/Validations/utils';

const FormDiv = styled.div`
    margin: 16px 0 24px;
    background: white;
`;

type Props = {
    volumeInfo?: VolumeAssertionBuilderState;
    value: VolumeAssertionBuilderState['parameters'];
    onChange: (newParams: VolumeAssertionBuilderState['parameters']) => void;
    updateVolumeAssertion: (newParams: VolumeAssertionBuilderState) => void;
    disabled?: boolean;
};

export const VolumeParametersBuilder = (props: Props) => {
    const form = Form.useFormInstance();
    const { volumeInfo } = props;
    const selectedType = volumeInfo?.type;
    if (selectedType === VolumeAssertionBuilderTypeOptions.AiInferredRowCountTotal) {
        return null;
    }
    const isRowCountChange = selectedType ? getIsRowCountChange(selectedType) : null;
    const propertyName = selectedType ? getPropertyFromVolumeType(selectedType) : '';
    const operator = selectedType ? volumeInfo?.[propertyName]?.operator : null;
    const showParameters = selectedType && operator && form.getFieldValue('volume-type');

    return showParameters ? (
        <FormDiv>
            {props.volumeInfo &&
                (isRowCountChange ? (
                    <VolumeRowCountChangeBuilder volumeInfo={props.volumeInfo} {...props} />
                ) : (
                    <VolumeRowCountTotalBuilder volumeInfo={props.volumeInfo} {...props} />
                ))}
        </FormDiv>
    ) : null;
};
