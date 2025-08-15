import DOMPurify from 'dompurify';

export const sanitizeRichText = (content: string) => {
    if (!content) return '';
    const encoded = content.replace(/</g, '&lt;').replace(/>/g, '&gt;');
    return DOMPurify.sanitize(encoded).replace(/&lt;/g, '<').replace(/&gt;/g, '>');
};
