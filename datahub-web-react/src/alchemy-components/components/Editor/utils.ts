import DOMPurify from 'dompurify';

export const sanitizeRichText = (content: string) => {
    if (!content) return '';
    const encoded = content.replace(/</g, '&lt;').replace(/>/g, '&gt;');
    return DOMPurify.sanitize(encoded).replace(/&lt;/g, '<').replace(/&gt;/g, '>');
};

export function ptToPx(pt: number): number {
    return Math.round((pt * 96) / 72);
}

export function pxToPt(px: number): number {
    return Math.round((px * 72) / 96);
}
