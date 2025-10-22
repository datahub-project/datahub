export type EditorProps = {
    readOnly?: boolean;
    content?: string;
    onChange?: (md: string) => void;
    className?: string;
    doNotFocus?: boolean;
    placeholder?: string;
    hideHighlightToolbar?: boolean;
    toolbarStyles?: React.CSSProperties;
    dataTestId?: string;
    onKeyDown?: (event: React.KeyboardEvent<HTMLDivElement>) => void;
    hideBorder?: boolean;
    uploadFile?: (file: File) => Promise<string>;
};
