import DOMPurify from 'dompurify';

export const sanitizeRichText = (content: string) => {
    if (!content) return '';
    const encoded = content.replace(/</g, '&lt;').replace(/>/g, '&gt;');
    return DOMPurify.sanitize(encoded).replace(/&lt;/g, '<').replace(/&gt;/g, '>');
};

export const flattenColors = (obj: Record<string, any>, prefix = ''): { key: string; value: string }[] => {
    return Object.entries(obj).flatMap(([key, val]) => {
        const newKey = prefix ? `${prefix}.${key}` : key;
        if (typeof val === 'string') {
            return [{ key: newKey, value: val }];
        }
        if (typeof val === 'object' && val !== null) {
            return flattenColors(val, newKey);
        }
        return [];
    });
};
