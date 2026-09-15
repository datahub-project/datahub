export interface MultiSelectInputProps {
    values: string[];
    onUpdate: (newValues: string[]) => void;
    placeholder?: string;
    label?: string;
    error?: string;
    helperText?: string;
    disabled?: boolean;
    inputTestId?: string;
    id?: string;
    className?: string;
    width?: string | number;
}
