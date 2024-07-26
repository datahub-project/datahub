import React from 'react';
import { Select } from 'antd';
import { Input, TextArea } from '@components';
import { FieldLabel, FlexBox, FormFieldsContainer } from './styledComponents';
import { FormType } from '../../../../types.generated';

const FormDetailsStep = () => {
    const formTypes = Object.values(FormType).map((type) => ({
        label: type.toString().charAt(0).toUpperCase() + type.slice(1).toLowerCase(),
        value: type.toString().toLowerCase(),
    }));

    return (
        <FormFieldsContainer>
            <FlexBox>
                <FieldLabel> Form Type</FieldLabel>
                <Select placeholder="Select Form Type" options={formTypes} />
            </FlexBox>
            <Input label="Form Name" placeholder="Enter Form Name" />
            <TextArea label="Description" placeholder="Add description here" />
        </FormFieldsContainer>
    );
};

export default FormDetailsStep;
