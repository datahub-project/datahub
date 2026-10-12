import { useCallback, useState } from 'react';

type FieldValidator<T> = (value: string, values: T) => string | undefined;

export type FieldValidators<T> = { [K in keyof T]: FieldValidator<T> };

export type AuthForm<T> = {
    values: T;
    /** Validation errors, only for fields the user has interacted with. */
    errors: Partial<Record<keyof T, string>>;
    isSubmitDisabled: boolean;
    setFieldValue: (field: keyof T, value: string) => void;
    setFieldValues: (fields: Partial<T>) => void;
    submit: () => void;
};

/**
 * Minimal form state for the auth pages: tracks values, which fields were touched,
 * and runs the validators on every change. Submission is blocked until every field
 * has been filled in and is valid.
 */
export function useAuthForm<T extends Record<string, string>>(
    initialValues: T,
    validators: FieldValidators<T>,
    onSubmit: (values: T) => void,
): AuthForm<T> {
    const [values, setValues] = useState<T>(initialValues);
    const [touched, setTouched] = useState<Partial<Record<keyof T, boolean>>>({});

    const fieldNames = Object.keys(initialValues) as (keyof T)[];

    const allErrors: Partial<Record<keyof T, string>> = {};
    const errors: Partial<Record<keyof T, string>> = {};
    fieldNames.forEach((field) => {
        const error = validators[field](values[field], values);
        if (error) {
            allErrors[field] = error;
            if (touched[field]) {
                errors[field] = error;
            }
        }
    });

    const hasErrors = Object.keys(allErrors).length > 0;
    const allTouched = fieldNames.every((field) => touched[field]);
    const isSubmitDisabled = hasErrors || !allTouched;

    const setFieldValue = useCallback((field: keyof T, value: string) => {
        setValues((prev) => ({ ...prev, [field]: value }));
        setTouched((prev) => ({ ...prev, [field]: true }));
    }, []);

    const setFieldValues = useCallback((fields: Partial<T>) => {
        setValues((prev) => ({ ...prev, ...fields }));
        setTouched((prev) => {
            const next = { ...prev };
            (Object.keys(fields) as (keyof T)[]).forEach((field) => {
                next[field] = true;
            });
            return next;
        });
    }, []);

    const submit = useCallback(() => {
        if (!isSubmitDisabled) {
            onSubmit(values);
        }
    }, [isSubmitDisabled, onSubmit, values]);

    return { values, errors, isSubmitDisabled, setFieldValue, setFieldValues, submit };
}
