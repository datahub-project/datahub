import React from 'react';
import styled from 'styled-components';

import {
    FILE_ATTRS,
    getFileTypeFromFilename,
    handleFileDownload,
} from '@components/components/Editor/extensions/fileDragDrop/fileUtils';
import { FileNode } from '@components/components/FileNode/FileNode';

import { safeUrl } from '@app/shared/urlUtils';

const VOID_TAGS = new Set([
    'area',
    'base',
    'br',
    'col',
    'embed',
    'hr',
    'img',
    'input',
    'link',
    'meta',
    'param',
    'source',
    'track',
    'wbr',
]);

const REACT_ATTR_NAMES: Record<string, string> = {
    class: 'className',
    for: 'htmlFor',
    colspan: 'colSpan',
    rowspan: 'rowSpan',
    tabindex: 'tabIndex',
};

const FileCard = styled.span`
    display: block;
    max-width: 100%;
`;

const PreviewImage = styled.img`
    display: block;
    max-width: 100%;
    margin-top: 8px;
`;

const PreviewFrame = styled.iframe`
    display: block;
    width: 100%;
    max-width: 100%;
    height: 400px;
    margin-top: 8px;
    border: none;
    border-radius: 8px;
`;

const PreviewVideo = styled.video`
    display: block;
    width: 50%;
    min-width: 150px;
    max-width: 100%;
    margin-top: 8px;
    border-radius: 8px;
`;

type FileCardProps = {
    url: string;
    name: string;
    type: string;
    size: string;
    id: string;
};

function ReadOnlyFileCard({ url, name, type, size, id }: FileCardProps) {
    const fileType = type || getFileTypeFromFilename(name);
    const previewSrc = safeUrl(url);
    const isImage = fileType.startsWith('image/');
    const isPdf = fileType === 'application/pdf';
    const isVideo = fileType.startsWith('video/');

    return (
        <FileCard
            className="file-node"
            data-testid={`file-node-${name}`}
            data-file-url={url}
            data-file-name={name}
            data-file-type={fileType}
            data-file-size={size}
            data-file-id={id}
        >
            <FileNode fileName={name} onClick={() => handleFileDownload(url, name)} />
            {isImage && previewSrc && <PreviewImage src={previewSrc} alt={name} />}
            {isPdf && previewSrc && <PreviewFrame src={previewSrc} title={name} />}
            {isVideo && previewSrc && (
                <PreviewVideo controls preload="metadata">
                    <source src={previewSrc} type={fileType} />
                </PreviewVideo>
            )}
        </FileCard>
    );
}

function elementAttributes(element: HTMLElement): Record<string, string> {
    const props: Record<string, string> = {};
    Array.from(element.attributes).forEach((attr) => {
        const name = attr.name.toLowerCase();
        if (name.startsWith('on') || name === 'style') return;
        props[REACT_ATTR_NAMES[name] ?? attr.name] = attr.value;
    });
    return props;
}

const TABLE_TAGS = new Set(['table', 'thead', 'tbody', 'tfoot', 'tr', 'colgroup']);

function nodeToReact(node: ChildNode, key: string, parentTag?: string): React.ReactNode {
    if (node.nodeType === Node.TEXT_NODE) {
        const text = node.textContent ?? '';
        if (!text || (parentTag && TABLE_TAGS.has(parentTag) && !text.trim())) return null;
        return <React.Fragment key={key}>{text}</React.Fragment>;
    }
    if (!(node instanceof HTMLElement)) return null;

    if (node.classList.contains('file-node')) {
        return (
            <ReadOnlyFileCard
                key={key}
                url={node.getAttribute(FILE_ATTRS.url) ?? ''}
                name={node.getAttribute(FILE_ATTRS.name) ?? ''}
                type={node.getAttribute(FILE_ATTRS.type) ?? ''}
                size={node.getAttribute(FILE_ATTRS.size) ?? ''}
                id={node.getAttribute(FILE_ATTRS.id) ?? ''}
            />
        );
    }

    const tag = node.tagName.toLowerCase();
    const meaningfulChildren = Array.from(node.childNodes).filter(
        (child) => !(child.nodeType === Node.TEXT_NODE && !child.textContent?.trim()),
    );
    // Marked wraps a file span in a paragraph. The card renders a div, which cannot sit inside that paragraph.
    if (
        tag === 'p' &&
        meaningfulChildren.length === 1 &&
        meaningfulChildren[0] instanceof HTMLElement &&
        meaningfulChildren[0].classList.contains('file-node')
    ) {
        return nodeToReact(meaningfulChildren[0], key, tag);
    }

    const props = { ...elementAttributes(node), key };
    if (VOID_TAGS.has(tag)) return React.createElement(tag, props);

    const children = Array.from(node.childNodes).map((child, index) => nodeToReact(child, `${key}-${index}`, tag));
    return React.createElement(tag, props, children);
}

/** Sanitized HTML, with file spans replaced by the file card. */
export function readOnlyHtmlToReact(html: string): React.ReactNode {
    if (typeof DOMParser === 'undefined') {
        return <div dangerouslySetInnerHTML={{ __html: html }} />;
    }
    const doc = new DOMParser().parseFromString(html, 'text/html');
    return Array.from(doc.body.childNodes).map((node, index) => nodeToReact(node, `readonly-${index}`));
}
