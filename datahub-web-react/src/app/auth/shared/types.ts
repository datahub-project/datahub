export type SignupFormValues = {
    fullName: string;
    email: string;
    password: string;
    confirmPassword: string;
};

export type LoginFormValues = {
    username: string;
    password: string;
};

export type ResetCredentialsFormValues = {
    email: string;
    password: string;
    confirmPassword: string;
};
