import { act } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';

import ManageFormContext from '@app/govern/Dashboard/Forms/ManageFormContext';
import { FormFields } from '@app/govern/Dashboard/Forms/formUtils';
import { useFormHandlers } from '@app/govern/Dashboard/Forms/useFormHandlers';

import { FormState, FormType } from '@types';

const Wrapper = ({ children }: { children: React.ReactChildren }) => {
    const [formValues, setFormValues] = React.useState<FormFields>({
        formName: 'test',
        formType: FormType.Completion,
        questions: [],
        actors: {
            owners: false,
            users: [],
            groups: [],
        },
        state: FormState.Draft,
        formSettings: undefined,
    });

    return (
        <ManageFormContext.Provider
            value={{
                formValues,
                setFormValues,
                form: undefined,
                data: undefined,
                formMode: 'create',
                setFormMode: () => {},
                isFormLoading: false,
                setIsFormLoading: () => {},
            }}
        >
            {children}
        </ManageFormContext.Provider>
    );
};

describe('useFormHandlers', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    test('handleInputChange updates the correct field in formValues', async () => {
        const { result } = renderHook(
            () => ({ handlers: useFormHandlers(), context: React.useContext(ManageFormContext) }),
            { wrapper: Wrapper },
        );

        await act(async () => {
            result.current.handlers.handleInputChange({
                target: { id: 'formName', value: 'New Form Name' },
            });
        });

        expect(result.current.context.formValues?.formName).toBe('New Form Name');
    });

    test('handleSelectChange updates the correct field in formValues', async () => {
        const { result } = renderHook(
            () => ({ handlers: useFormHandlers(), context: React.useContext(ManageFormContext) }),
            { wrapper: Wrapper },
        );

        await act(async () => {
            result.current.handlers.handleSelectChange('formType', 'Incident');
        });

        expect(result.current.context.formValues?.formType).toBe('Incident');
    });

    test('handleOwnersCheckBox toggles the owners flag', async () => {
        const { result } = renderHook(
            () => ({ handlers: useFormHandlers(), context: React.useContext(ManageFormContext) }),
            { wrapper: Wrapper },
        );

        await act(async () => {
            result.current.handlers.handleOwnersCheckBox({ target: { checked: true } });
        });

        expect(result.current.context.formValues?.actors?.owners).toBe(true);
    });

    test('handleNotifyAsigneesCheckBox sets notifyAssigneesOnPublish correctly', async () => {
        const { result } = renderHook(
            () => ({ handlers: useFormHandlers(), context: React.useContext(ManageFormContext) }),
            { wrapper: Wrapper },
        );

        await act(async () => {
            result.current.handlers.handleNotifyAsigneesCheckBox(true);
        });

        expect(result.current.context.formValues?.formSettings?.notificationSettings?.notifyAssigneesOnPublish).toBe(
            true,
        );

        await act(async () => {
            result.current.handlers.handleNotifyAsigneesCheckBox(false);
        });

        expect(result.current.context.formValues?.formSettings?.notificationSettings?.notifyAssigneesOnPublish).toBe(
            false,
        );
    });
});
