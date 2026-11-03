import removeMd from '@tommoor/remove-markdown';

// Flattens markdown into one line of plain text, joining paragraphs with " • ".
export function stripMarkdown(text: string) {
    return removeMd(text, {
        stripListLeaders: true,
        gfm: true,
        useImgAltText: true,
    })
        .replace(/\n*\n/g, ' • ')
        .replace(/^•/, '');
}
