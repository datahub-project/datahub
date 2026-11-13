// Transaction meta key used by DetailsExtension to mark a details open/close toggle.
// OnChangeMarkdown ignores transactions carrying this key so that expanding/collapsing
// a section does not trigger an autosave or mark the document as dirty.
export const DETAILS_TOGGLE_META = 'detailsToggle';

export type EditorMentionsPickerProps = {
    /** When true, renders the dropdown outside the editor to avoid overflow clipping */
    renderOutsideEditor?: boolean;
};

/** A stored mention. Free of Remirror types, so the read-only view can render it too. */
export type EditorMentionViewProps = {
    urn: string;
    name: string;
};

export type EditorMentionsProviderProps = {
    /** URNs of the mentions in the content, in document order, without duplicates. */
    urns: string[];
    children: React.ReactNode;
};

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
    /**
     * Reject edits while keeping the Remirror surface mounted. `readOnly` swaps in the
     * static HTML renderer, which changes padding and placeholder placement.
     */
    editingDisabled?: boolean;
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
    /**
     * Replaces the default @-mentions dropdown. Rendered inside the Remirror context, so it can
     * drive the `datahubMention` autocomplete itself.
     */
    mentionsComponent?: React.ReactNode;
    /**
     * The default @-mentions picker, also the fallback when `mentionsComponent` fails. Rendered inside
     * the Remirror context, so it can drive the `datahubMention` autocomplete. Without it typing @
     * opens no picker.
     */
    mentionsPicker?: React.ComponentType<EditorMentionsPickerProps>;
    /** Renders a mention, both while editing and in read-only mode. Without it a mention renders as `@name`. */
    mentionView?: React.ComponentType<EditorMentionViewProps>;
    /**
     * Read-only mode only: wraps content that has mentions and receives their URNs, so `mentionView`
     * can read entities fetched once for the whole content instead of one request per mention.
     */
    mentionsProvider?: React.ComponentType<EditorMentionsProviderProps>;
    /** Lets the toolbar's image button upload files when `uploadFileProps.onFileUpload` is set. */
    imageUploadEnabled?: boolean;
};
