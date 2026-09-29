export const getInputType = (type?: string, isPassword?: boolean, showPassword?: boolean) => {
    if (isPassword) return showPassword ? 'text' : 'password';
    if (type) return type;
    return 'text';
};
