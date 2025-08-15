export const getInputType = (type?: string, isPassword?: boolean, showPassword?: boolean) => {
    if (type) return type;
    if (isPassword && !showPassword) return 'password';
    return 'text';
};
