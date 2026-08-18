import { ImportUseCase } from '@app/context/import/import.types';

import { DocumentImportUseCase } from '@types';

export const TEXT_EXTENSIONS = new Set(['.md', '.markdown', '.txt', '.rst', '.csv', '.json', '.yaml', '.yml']);
export const HTML_EXTENSIONS = new Set(['.html', '.htm']);

export function getExtension(name: string): string {
    const dot = name.lastIndexOf('.');
    return dot >= 0 ? name.substring(dot).toLowerCase() : '';
}

export function stripHtmlTags(html: string): string {
    return html
        .replace(/<[^>]*>/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

export async function extractDocxText(arrayBuffer: ArrayBuffer): Promise<string | null> {
    const mammoth = await import('mammoth');
    const result = await mammoth.extractRawText({ arrayBuffer });
    return result.value || null;
}

export async function extractTextFromFile(file: File): Promise<string | null> {
    const ext = getExtension(file.name);

    if (TEXT_EXTENSIONS.has(ext)) {
        return file.text();
    }
    if (HTML_EXTENSIONS.has(ext)) {
        const html = await file.text();
        return stripHtmlTags(html);
    }
    if (ext === '.docx') {
        const arrayBuffer = await file.arrayBuffer();
        return extractDocxText(arrayBuffer);
    }
    return null;
}

export function mapImportUseCase(useCase: ImportUseCase): DocumentImportUseCase {
    return useCase === ImportUseCase.SKILL ? DocumentImportUseCase.Skill : DocumentImportUseCase.ContextDocument;
}
