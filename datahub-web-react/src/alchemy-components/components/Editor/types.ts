/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

export type FileUploadSource = 'drag-and-drop' | 'button';

export enum FileUploadFailureType {
    FILE_SIZE = 'file_size',
    FILE_TYPE = 'file_type',
    UPLOADING_NOT_SUPPORTED = 'uploading_not_supported',
    UNKNOWN = 'unknown',
}

export interface FileUploadProps {
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
    onKeyDown?: (event: React.KeyboardEvent<HTMLDivElement>) => void;
    hideBorder?: boolean;
    uploadFileProps?: FileUploadProps;
};
