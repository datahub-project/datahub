import { Input, SimpleSelect, TextArea } from '@src/alchemy-components';
import { FormState, FormType } from '@src/types.generated';
import { Form } from 'antd';
import React, { useContext } from 'react';
import ManageFormContext from './ManageFormContext';
import { FieldLabel, FormFieldsContainer } from './styledComponents';
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
                    <Input
                        placeholder="Form name"
                        onChange={handleInputChange}
                        label="Name"
                        id="formName"
                        data-testid="name-input"
                        required
                    />
                </Form.Item>
                <Form.Item name="formDescription">
                    <TextArea
                        label="Description"
                        placeholder="Form description"
                        onChange={handleInputChange}
                        data-testid="description-textarea"
                    />
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
                    <SimpleSelect
                        onUpdate={(values) => handleSelectChange('formType', values[0])}
                        placeholder="Select Form Type"
                        options={formTypes}
                        values={formValues?.formType ? [formValues?.formType] : [FormType.Completion]}
                        isDisabled={formValues.state !== FormState.Draft}
                        width="full"
                        showDescriptions
                        showClear={false}
                    />
                </Form.Item>
            </FormFieldsContainer>
        </Form>
    );
};

export default DetailsForm;
