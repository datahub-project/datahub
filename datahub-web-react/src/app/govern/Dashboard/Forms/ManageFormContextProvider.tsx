import React, { useEffect, useState } from 'react';
import { Form } from 'antd';
import { useParams } from 'react-router';
import ManageFormContext from './ManageFormContext';
import { FormType } from '../../../../types.generated';
import { FormFields, FormMode } from './formUtils';
import { useGetFormQuery } from '../../../../graphql/form.generated';

export const ManageFormContextProvider = ({ children }: { children: React.ReactNode }) => {
    const defaultValues: FormFields = {
        formType: FormType.Verification,
        formName: '',
    };
    const [currentStep, setCurrentStep] = useState(1);
    const [formValues, setFormValues] = useState(defaultValues);
    const [formMode, setFormMode] = useState<FormMode>('create');
    const [isFormLoading, setIsFormLoading] = useState<boolean>(false);

    const [form] = Form.useForm();

    const { urn } = useParams<{ urn: string }>();

    const { data, loading } = useGetFormQuery({
        variables: {
            urn,
        },
        skip: !urn,
    });

    useEffect(() => {
        setIsFormLoading(loading);
    }, [loading, setIsFormLoading]);

    useEffect(() => {
        if (data) {
            const formData = data.form;
            const values: FormFields = {
                formType: formData?.info.type as FormType,
                formName: formData?.info.name as string,
                formDescription: formData?.info.description as string | undefined,
            };
            setFormValues(values);
            form?.setFieldsValue(values);
        }
    }, [data, form, setFormValues]);

    return (
        <ManageFormContext.Provider
            value={{
                currentStep,
                setCurrentStep,
                formValues,
                setFormValues,
                form,
                formMode,
                setFormMode,
                isFormLoading,
                setIsFormLoading,
            }}
        >
            {children}
        </ManageFormContext.Provider>
    );
};
