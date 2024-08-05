import React, { useContext } from 'react';
import { Form, Input, Select } from 'antd';
import { TextArea } from '@components';
import { FieldLabel, FormFieldsContainer } from './styledComponents';
import { FormType } from '../../../../types.generated';
import ManageFormContext from './ManageFormContext';
import { useFormHandlers } from './useFormHandlers';

const FormDetailsStep = () => {
    const { form } = useContext(ManageFormContext);
    const formTypes = Object.values(FormType).map((type) => ({
        label: type.toString().charAt(0).toUpperCase() + type.slice(1).toLowerCase(),
        value: type,
    }));

    const { handleInputChange } = useFormHandlers();

    return (
        <Form form={form}>
            <FormFieldsContainer>
                <FieldLabel> Form Type</FieldLabel>
                <Form.Item
                    name="formType"
                    rules={[
                        {
                            required: true,
                            message: 'Please select the form type',
                        },
                    ]}
                >
                    <Select placeholder="Select Form Type" options={formTypes} />
                </Form.Item>
                <FieldLabel> Form Name</FieldLabel>
                <Form.Item
                    name="formName"
                    rules={[
                        {
                            required: true,
                            message: 'Please enter the form name',
                        },
                    ]}
                >
                    <Input placeholder="Enter Form Name" onChange={handleInputChange} />
                </Form.Item>
                <Form.Item name="formDescription">
                    <TextArea label="Description" placeholder="Add description here" onChange={handleInputChange} />
                </Form.Item>
            </FormFieldsContainer>
        </Form>
    );
};

export default FormDetailsStep;
