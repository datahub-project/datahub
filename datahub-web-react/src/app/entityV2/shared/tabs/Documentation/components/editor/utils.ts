import DOMPurify from 'dompurify';

export const sanitizeRichText = (content: string) => {
    const encoded = content.replace(/</g, '&lt;').replace(/>/g, '&gt;');
    return DOMPurify.sanitize(encoded).replace(/&lt;/g, '<').replace(/&gt;/g, '>');
};