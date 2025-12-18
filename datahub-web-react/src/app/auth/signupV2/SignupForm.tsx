import { Input } from '@components';
import { Form, FormInstance } from 'antd';
import React, { useEffect } from 'react';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import { SignupFormValues } from '@app/auth/shared/types';
import { FieldLabel } from '@app/sharedV2/forms/FieldLabel';

const FormContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 32px;
    padding: 0 20px;
`;

const ItemContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

interface Props {
    form: FormInstance;
    handleSubmit: (values: SignupFormValues) => void;
    onFormChange: () => void;
    isSubmitDisabled: boolean;
}

export default function SignupForm({ form, handleSubmit, onFormChange, isSubmitDisabled }: Props) {
    const location = useLocation();

    const searchParams = new URLSearchParams(location.search);

    const emailFromQuery = searchParams.get('email');
    const firstNameFromQuery = searchParams.get('first_name');
    const lastNameFromQuery = searchParams.get('last_name');

    const isEmailFromQuery = Boolean(emailFromQuery);

    useEffect(() => {
        form.setFieldsValue({
            email: emailFromQuery || undefined,
            fullName:
                firstNameFromQuery || lastNameFromQuery
                    ? `${firstNameFromQuery ?? ''} ${lastNameFromQuery ?? ''}`.trim()
                    : undefined,
        });
    }, [emailFromQuery, firstNameFromQuery, lastNameFromQuery, form]);

    const handleKeyDown = (e: React.KeyboardEvent) => {
        if (e.key === 'Enter' && !isSubmitDisabled) {
            form.submit();
        }
    };

    return (
        <FormContainer>
            <Form form={form} onFinish={handleSubmit} onFieldsChange={onFormChange} onKeyDown={handleKeyDown}>
                <ItemContainer>
                    <FieldLabel label="Email" required />
                    <Form.Item rules={[{ required: true, message: 'Please fill in your email' }]} name="email">
                        <Input placeholder="name@company.com" isDisabled={isEmailFromQuery} inputTestId="email" />
                    </Form.Item>
                </ItemContainer>

                <ItemContainer>
                    <FieldLabel label="Full Name" required />
                    <Form.Item rules={[{ required: true, message: 'Please fill in your name' }]} name="fullName">
                        <Input placeholder="First name Last name" inputTestId="name" />
                    </Form.Item>
                </ItemContainer>

                <ItemContainer>
                    <FieldLabel label="Password" required />
                    <Form.Item
                        rules={[
                            { required: true, message: 'Please fill in your password' },
                            ({ getFieldValue }) => ({
                                validator() {
                                    if (getFieldValue('password').length < 8) {
                                        return Promise.reject(new Error('Must be 8 characters long; case sensitive'));
                                    }
                                    return Promise.resolve();
                                },
                            }),
                        ]}
                        name="password"
                    >
                        <Input placeholder="********" type="password" inputTestId="password" />
                    </Form.Item>
                </ItemContainer>

                <ItemContainer>
                    <FieldLabel label="Confirm Password" required />
                    <Form.Item
                        rules={[
                            { required: true, message: 'Please confirm your password' },
                            ({ getFieldValue }) => ({
                                validator() {
                                    if (getFieldValue('confirmPassword') !== getFieldValue('password')) {
                                        return Promise.reject(new Error('Your passwords do not match'));
                                    }
                                    return Promise.resolve();
                                },
                            }),
                        ]}
                        name="confirmPassword"
                    >
                        <Input placeholder="********" type="password" inputTestId="confirmPassword" />
                    </Form.Item>
                </ItemContainer>
            </Form>
        </FormContainer>
    );
}
