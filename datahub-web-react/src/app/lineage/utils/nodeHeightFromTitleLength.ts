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

export function nodeHeightFromTitleLength(title?: string) {
    return Math.floor(calcualteSize(title || DEFAULT_TEXT_TO_GET_NON_ZERO_HEIGHT).height) + HEIGHT_WITHOUT_TEXT_HEIGHT;
}
