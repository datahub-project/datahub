export enum ImportUseCase {
    CONTEXT_DOCUMENT = 'CONTEXT_DOCUMENT',
    SKILL = 'SKILL',
}

export enum ImportSourceType {
    FILE_UPLOAD = 'FILE_UPLOAD',
    GITHUB = 'GITHUB',
    NOTION = 'NOTION',
    CONFLUENCE = 'CONFLUENCE',
}

/** Context docs import always creates editable native documents by default. */
export const CONTEXT_DOCUMENT_IMPORT_MODE = 'NATIVE' as const;

export type ImportStep = 'source' | 'configure' | 'importing' | 'result';
