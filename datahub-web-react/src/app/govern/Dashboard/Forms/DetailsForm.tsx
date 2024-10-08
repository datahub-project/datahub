import { Input, Text, TextArea } from '@src/alchemy-components';
import { FormState, FormType } from '@src/types.generated';
import { Form, Select } from 'antd';
import React, { useContext } from 'react';
import ManageFormContext from './ManageFormContext';
import {
    CustomDropdown,
    FieldLabel,
    FormFieldsContainer,
    SelectOptionContainer,
    StyledSelect,
} from './styledComponents';
import { useFormHandlers } from './useFormHandlers';

const DetailsForm = () => {
    const { form, formValues } = useContext(ManageFormContext);

    const { handleInputChange, handleSelectChange } = useFormHandlers();

    const formTypes = Object.values(FormType).map((type) => ({
        label: type.toString().charAt(0).toUpperCase() + type.slice(1).toLowerCase(),
        value: type,
        description:
            type === FormType.Completion
                ? 'Collect required information for specific data assets'
                : 'Collect and require certification for required attributes',
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
                    <Input placeholder="Form name" onChange={handleInputChange} label="Name" id="formName" required />
                </Form.Item>
                <Form.Item name="formDescription">
                    <TextArea label="Description" placeholder="Form description" onChange={handleInputChange} />
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
                    <StyledSelect
                        placeholder="Select Form Type"
                        onChange={(value) => handleSelectChange('formType', value)}
                        disabled={formValues.state !== FormState.Draft}
                        dropdownRender={(menu) => <CustomDropdown>{menu}</CustomDropdown>}
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
                    </StyledSelect>
                </Form.Item>
            </FormFieldsContainer>
        </Form>
    );
};

export default DetailsForm;
