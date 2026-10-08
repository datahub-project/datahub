const MIN_PASSWORD_LENGTH = 8;

export function required(message: string) {
    return (value: string) => (value ? undefined : message);
}

export function password(messages: { required: string; tooShort: string }) {
    return (value: string) => {
        if (!value) return messages.required;
        if (value.length < MIN_PASSWORD_LENGTH) return messages.tooShort;
        return undefined;
    };
}

export function confirmPassword(messages: { required: string; mismatch: string }) {
    return (value: string, values: { password: string }) => {
        if (!value) return messages.required;
        if (value !== values.password) return messages.mismatch;
        return undefined;
    };
}
