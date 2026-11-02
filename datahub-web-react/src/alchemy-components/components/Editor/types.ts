// Transaction meta key used by DetailsExtension to mark a details open/close toggle.
// OnChangeMarkdown ignores transactions carrying this key so that expanding/collapsing
// a section does not trigger an autosave or mark the document as dirty.
export const DETAILS_TOGGLE_META = 'detailsToggle';

export type FileUploadSource = 'drag-and-drop' | 'button';

export enum FileUploadFailureType {
    FILE_SIZE = 'file_size',
    FILE_TYPE = 'file_type',
    UPLOADING_NOT_SUPPORTED = 'uploading_not_supported',
    UNKNOWN = 'unknown',
}

interface FileUploadProps {
    onFileUpload?: (file: File) => Promise<string>;
    onFileUploadAttempt?: (fileType: string, fileSize: number, source: FileUploadSource) => void;
    onFileUploadFailed?: (
        fileType: string,
        fileSize: number,
        source: FileUploadSource,
        failureType: FileUploadFailureType,
        comment?: string,
    ) => void;
    onFileUploadSucceeded?: (fileType: string, fileSize: number, source: FileUploadSource) => void;
    onFileDownloadView?: (fileType: string, fileSize: number) => void;
}

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
    /** Key down handler - fires in capture phase to allow intercepting before ProseMirror */
    onKeyDown?: (event: React.KeyboardEvent<HTMLDivElement>) => void;
    /** Paste handler - fires in capture phase to allow intercepting before ProseMirror */
    onPaste?: (event: React.ClipboardEvent<HTMLDivElement>) => void;
    hideBorder?: boolean;
    uploadFileProps?: FileUploadProps;
    fixedBottomToolbar?: boolean;
    /** Optional content rendered below the formatting buttons inside the toolbar card. */
    belowToolbar?: React.ReactNode;
    /** Hide the formatting toolbar completely (for chat input use case) */
    hideToolbar?: boolean;
    /** Enable compact mode with smaller min-height and adjusted padding */
    compact?: boolean;
};
