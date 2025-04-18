import { Form } from 'antd';
import React, { useEffect, useState } from 'react';
import { useParams } from 'react-router';

import ManageFormContext from '@app/govern/Dashboard/Forms/ManageFormContext';
import { FormActors, FormFields, FormMode } from '@app/govern/Dashboard/Forms/formUtils';

import { useGetFormQuery } from '@graphql/form.generated';
import { FormPrompt, FormState, FormType } from '@types';

export const ManageFormContextProvider = ({ children }: { children: React.ReactNode }) => {
    const defaultValues: FormFields = {
        formName: undefined,
        formType: undefined,
        questions: [],
        actors: {
            owners: false,
            users: [],
            groups: [],
        },
        state: FormState.Draft,
    };
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
                questions: (formData?.info.prompts as FormPrompt[]) || [],
                actors: formData?.info.actors as FormActors,
                state: formData?.info?.status?.state,
                assets: {
                    logicalPredicate:
                        formData?.dynamicFormAssignment?.json && JSON.parse(formData?.dynamicFormAssignment?.json),
                },
            };
            setFormValues(values);
            form?.setFieldsValue(values);
        } else form?.setFieldsValue({ formType: FormType.Completion });
    }, [data, form, setFormValues]);

    return (
        <ManageFormContext.Provider
            value={{
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
