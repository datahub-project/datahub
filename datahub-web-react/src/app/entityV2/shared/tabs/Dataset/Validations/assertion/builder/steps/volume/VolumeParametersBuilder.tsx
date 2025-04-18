import React from 'react';
import styled from 'styled-components';
import { Form, Typography } from 'antd';
import { getParameterBuilderTitle } from './utils';
import { VolumeRowCountTotalBuilder } from './VolumeRowCountTotalBuilder';
import { VolumeRowCountChangeBuilder } from './VolumeRowCountChangeBuilder';
import { getIsRowCountChange, getPropertyFromVolumeType } from '../../../../utils';
import { VolumeAssertionBuilderState, VolumeAssertionBuilderTypeOptions } from '../../types';

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
    const title = selectedType && operator ? getParameterBuilderTitle(selectedType, operator) : '';
    const showParameters = selectedType && operator && form.getFieldValue('volume-type');

    return showParameters ? (
        <FormDiv>
            <Typography.Title level={5}>{title}</Typography.Title>
            {props.volumeInfo &&
                (isRowCountChange ? (
                    <VolumeRowCountChangeBuilder volumeInfo={props.volumeInfo} {...props} />
                ) : (
                    <VolumeRowCountTotalBuilder volumeInfo={props.volumeInfo} {...props} />
                ))}
        </FormDiv>
    ) : null;
};
