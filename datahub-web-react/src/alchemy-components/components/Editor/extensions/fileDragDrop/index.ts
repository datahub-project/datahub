// Callers that only need a helper should import fileUtils. This module also loads
// the editor extension, and that pulls the syntax highlighter into the page.
export { FileDragDropExtension } from './FileDragDropExtension';
export {
    // FileNodeAttributes,
    SUPPORTED_FILE_TYPES,
    createFileNodeAttributes,
    validateFile,
    isFileUrl,
} from './fileUtils';
