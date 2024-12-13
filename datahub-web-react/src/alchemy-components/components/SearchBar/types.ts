export interface SearchBarProps {
    placeholder?: string;
    value?: string;
    width?: string;
    onChange?: (value: string) => void;
    allowClear?: boolean;
}
