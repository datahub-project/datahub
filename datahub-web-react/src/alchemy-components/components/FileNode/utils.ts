import { FileArrowDown } from '@phosphor-icons/react/dist/csr/FileArrowDown';
import { FileAudio } from '@phosphor-icons/react/dist/csr/FileAudio';
import { FileCode } from '@phosphor-icons/react/dist/csr/FileCode';
import { FileCsv } from '@phosphor-icons/react/dist/csr/FileCsv';
import { FileDoc } from '@phosphor-icons/react/dist/csr/FileDoc';
import { FileHtml } from '@phosphor-icons/react/dist/csr/FileHtml';
import { FileImage } from '@phosphor-icons/react/dist/csr/FileImage';
import { FileJpg } from '@phosphor-icons/react/dist/csr/FileJpg';
import { FilePdf } from '@phosphor-icons/react/dist/csr/FilePdf';
import { FilePng } from '@phosphor-icons/react/dist/csr/FilePng';
import { FilePpt } from '@phosphor-icons/react/dist/csr/FilePpt';
import { FilePy } from '@phosphor-icons/react/dist/csr/FilePy';
import { FileSvg } from '@phosphor-icons/react/dist/csr/FileSvg';
import { FileText } from '@phosphor-icons/react/dist/csr/FileText';
import { FileVideo } from '@phosphor-icons/react/dist/csr/FileVideo';
import { FileXls } from '@phosphor-icons/react/dist/csr/FileXls';
import { FileZip } from '@phosphor-icons/react/dist/csr/FileZip';
import React from 'react';

/**
 * Get icon component to show based on file extension
 * @param extension - the extension of the file
 * @returns Phosphor icon component for the file type
 */
export const getFileIconFromExtension = (extension: string): React.ComponentType<any> => {
    switch (extension.toLowerCase()) {
        case 'pdf':
            return FilePdf;
        case 'doc':
        case 'docx':
            return FileDoc;
        case 'txt':
        case 'md':
        case 'rtf':
        case 'log':
        case 'json':
            return FileText;
        case 'xls':
        case 'xlsx':
            return FileXls;
        case 'ppt':
        case 'pptx':
            return FilePpt;
        case 'svg':
            return FileSvg;
        case 'jpg':
        case 'jpeg':
            return FileJpg;
        case 'png':
            return FilePng;
        case 'gif':
        case 'webp':
        case 'bmp':
        case 'tiff':
            return FileImage;
        case 'mp4':
        case 'wmv':
        case 'mov':
            return FileVideo;
        case 'mp3':
            return FileAudio;
        case 'zip':
        case 'rar':
        case 'gz':
            return FileZip;
        case 'csv':
            return FileCsv;
        case 'html':
            return FileHtml;
        case 'py':
            return FilePy;
        case 'java':
            return FileCode;
        default:
            return FileArrowDown;
    }
};
