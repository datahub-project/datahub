import { Text, TextArea, Input } from '@src/alchemy-components';
import { FormState, FormType } from '@src/types.generated';
import { Form, Select } from 'antd';
import React, { useContext } from 'react';
import ManageFormContext from './ManageFormContext';
import { FieldLabel, FormFieldsContainer, SelectOptionContainer } from './styledComponents';
import { useFormHandlers } from './useFormHandlers';

const DetailsForm = () => {
    const { form, formValues } = useContext(ManageFormContext);

    const { handleInputChange, handleSelectChange } = useFormHandlers();

    const formTypes = Object.values(FormType).map((type) => ({
        label: type.toString().charAt(0).toUpperCase() + type.slice(1).toLowerCase(),
        value: type,
        description:
            type === FormType.Completion
                ? 'Crowdsource required attributes for your data assets.'
                : 'Collect and formally verify required attributes for your data assets.',
    }));
    return (
        <Form form={form}>
            <FormFieldsContainer>
                <Form.Item
                    name="formName"
                    rules={[
                        {
                            required: true,
                            message: 'Please enter the form name',
                        },
                    ]}
                >
                    <Input
                        placeholder="Add form name"
                        onChange={handleInputChange}
                        label="Name"
                        id="formName"
                        required
                    />
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
                        onChange={(value) => handleSelectChange('formType', value)}
                        disabled={formValues.state !== FormState.Draft}
                    >
                        {formTypes.map((option) => {
                            return (
                                <Select.Option key={option.value} value={option.value}>
                                    <SelectOptionContainer>
                                        <Text color="gray" weight="semiBold" size="md">
                                            {option.label}
                                        </Text>
                                        <Text color="gray" size="sm">
                                            {option.description}
                                        </Text>
                                    </SelectOptionContainer>
                                </Select.Option>
                            );
                        })}
                    </Select>
                </Form.Item>
            </FormFieldsContainer>
        </Form>
    );
};

export default DetailsForm;
