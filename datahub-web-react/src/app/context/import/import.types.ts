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

export type ImportStep = 'source' | 'configure' | 'importing' | 'result';
