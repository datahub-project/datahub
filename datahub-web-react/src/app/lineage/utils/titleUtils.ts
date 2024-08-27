import { SchemaField } from '../../../types.generated';
import { COLUMN_HEIGHT, EXPAND_COLLAPSE_COLUMNS_TOGGLE_HEIGHT, NUM_COLUMNS_PER_PAGE } from '../constants';

interface OptionalOptions {
    font?: string;
    fontSize?: string;
    fontWeight?: string;
    lineHeight?: string;
    width?: string;
    wordBreak?: string;
}

interface Options {
    font: string;
    fontSize: string;
    fontWeight: string;
    lineHeight: string;
    width: string;
    wordBreak: string;
}

interface Size {
    width: number;
    height: number;
}

const HEIGHT_WITHOUT_TEXT_HEIGHT = 66;
const DEFAULT_TEXT_TO_GET_NON_ZERO_HEIGHT = 'a';
export const NODE_WIDTH_WITHOUT_TITLE = 61;

function createDummyElement(text: string, options: Options): HTMLElement {
    const element = document.createElement('div');
    const textNode = document.createTextNode(text);

    element.appendChild(textNode);

    element.style.fontFamily = options.font;
    element.style.fontSize = options.fontSize;
    element.style.fontWeight = options.fontWeight;
    element.style.lineHeight = options.lineHeight;
    element.style.position = 'absolute';
    element.style.visibility = 'hidden';
    element.style.left = '-999px';
    element.style.top = '-999px';
    element.style.width = options.width;
    element.style.height = 'auto';
    element.style.wordBreak = options.wordBreak;

    document.body.appendChild(element);

    return element;
}

function destroyElement(element: HTMLElement): void {
    element?.parentNode?.removeChild(element);
}

const cache = {};

const calcualteSize = (text: string, options: OptionalOptions = {}): Size => {
    const cacheKey = JSON.stringify({ text, options });

    if (cache[cacheKey]) {
        return cache[cacheKey];
    }

    // prepare options
    const finalOptions: OptionalOptions = {};
    finalOptions.font = options.font || 'Manrope';
    finalOptions.fontSize = options.fontSize || '14px';
    finalOptions.fontWeight = options.fontWeight || 'normal';
    finalOptions.lineHeight = options.lineHeight || 'normal';
    finalOptions.width = options.width || '125px';
    finalOptions.wordBreak = options.wordBreak || 'break-all';

    const element = createDummyElement(text, finalOptions as Options);

    const size = {
        width: element.offsetWidth,
        height: element.offsetHeight,
    };

    destroyElement(element);

    cache[cacheKey] = size;

    return size;
};

export function getTitleHeight(title?: string) {
    return Math.floor(calcualteSize(title || DEFAULT_TEXT_TO_GET_NON_ZERO_HEIGHT).height);
}

export function nodeHeightFromTitleLength(
    title?: string,
    fields?: SchemaField[],
    showColumns?: boolean,
    collapsed?: boolean,
) {
    let showColumnBuffer = 0;
    let columnPaginationBuffer = 0;
    if (showColumns && fields) {
        if (!collapsed) {
            showColumnBuffer =
                Math.min(fields.length, NUM_COLUMNS_PER_PAGE) * COLUMN_HEIGHT + EXPAND_COLLAPSE_COLUMNS_TOGGLE_HEIGHT;
            if (fields.length > NUM_COLUMNS_PER_PAGE) {
                columnPaginationBuffer = 40;
            }
        } else {
            showColumnBuffer = EXPAND_COLLAPSE_COLUMNS_TOGGLE_HEIGHT;
        }
    }
    return getTitleHeight(title) + HEIGHT_WITHOUT_TEXT_HEIGHT + showColumnBuffer + columnPaginationBuffer;
}

function truncate(input, length) {
    if (!input) return '';
    if (input.length > length) {
        return `${input.substring(0, length)}...`;
    }
    return input;
}

function getLastTokenOfTitle(title?: string): string {
    if (!title) return '';

    const lastToken = title?.split('.').slice(-1)[0];

    // if the last token does not contain any content, the string should not be tokenized on `.`
    if (lastToken.replace(/\s/g, '').length === 0) {
        return title;
    }

    return lastToken;
}

export function getShortenedTitle(title: string, nodeWidth: number) {
    const titleWidth = nodeWidth - NODE_WIDTH_WITHOUT_TITLE;
    return truncate(getLastTokenOfTitle(title), Math.ceil(titleWidth / 10));
}
