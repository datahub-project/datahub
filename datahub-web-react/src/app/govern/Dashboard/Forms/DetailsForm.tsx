import { TextArea } from '@src/alchemy-components';
import { FormType } from '@src/types.generated';
import { Form, Input, Select } from 'antd';
import React, { useContext } from 'react';
import ManageFormContext from './ManageFormContext';
import { FieldLabel, FormFieldsContainer } from './styledComponents';
import { useFormHandlers } from './useFormHandlers';

const DetailsForm = () => {
    const { form } = useContext(ManageFormContext);

    const { handleInputChange, handleSelectChange } = useFormHandlers();

    const formTypes = Object.values(FormType).map((type) => ({
        label: type.toString().charAt(0).toUpperCase() + type.slice(1).toLowerCase(),
        value: type,
    }));
    return (
        <Form form={form}>
            <FormFieldsContainer>
                <FieldLabel> Name</FieldLabel>
                <Form.Item
                    name="formName"
                    rules={[
                        {
                            required: true,
                            message: 'Please enter the form name',
                        },
                    ]}
                >
                    <Input placeholder="Add form name" onChange={handleInputChange} required />
                </Form.Item>
                <Form.Item name="formDescription">
                    <TextArea label="Description" placeholder="Add description here" onChange={handleInputChange} />
                </Form.Item>
                <FieldLabel> Type</FieldLabel>
                <Form.Item
                    name="formType"
                    rules={[
                        {
                            required: true,
                            message: 'Please select the form type',
                        },
                    ]}
                >
                    <Select
                        placeholder="Select Form Type"
                        options={formTypes}
                        onChange={(value) => handleSelectChange('formType', value)}
                    />
                </Form.Item>
            </FormFieldsContainer>
        </Form>
    );
};

export default DetailsForm;
