import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { Button } from '@components/components/Button';
import {
    FILE_ATTRS,
    TEXT_FILE_TYPES_TO_PREVIEW,
    getFileTypeFromFilename,
    handleFileDownload,
} from '@components/components/Editor/extensions/fileDragDrop/fileUtils';
import { isSafeHref } from '@components/components/Editor/readOnlyHtml';
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

const FileCard = styled.div`
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

const PreviewText = styled.pre`
    max-height: 400px;
    margin-top: 8px;
    overflow: auto;
    white-space: pre-wrap;
`;

const InlineFileChip = styled.button`
    display: inline;
    padding: 0;
    border: 0;
    background: none;
    color: inherit;
    font: inherit;
    cursor: pointer;
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
    const previewSrc = isSafeHref(url) ? safeUrl(url) : '';
    const isImage = fileType.startsWith('image/');
    const isPdf = fileType === 'application/pdf';
    const isVideo = fileType.startsWith('video/');
    const isText = TEXT_FILE_TYPES_TO_PREVIEW.some((prefix) => fileType.startsWith(prefix));
    const canPreview = Boolean(previewSrc) && (isImage || isPdf || isVideo || isText);
    // Collapsed until asked. A line-clamped card (search, schema, announcement) must not
    // mount a 400px iframe or fetch the file on first paint.
    const [open, setOpen] = useState(false);
    const [text, setText] = useState<string | null>(null);

    useEffect(() => {
        if (!open || !isText || !previewSrc) return undefined;
        let cancelled = false;
        fetch(previewSrc)
            .then((response) => (response.ok ? response.text() : Promise.reject(new Error('preview failed'))))
            .then((body) => {
                if (!cancelled) setText(body);
            })
            .catch(() => {
                if (!cancelled) setText(null);
            });
        return () => {
            cancelled = true;
        };
    }, [open, isText, previewSrc]);

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
            <FileNode
                fileName={name}
                onClick={() => handleFileDownload(url, name)}
                extraRightContent={
                    canPreview ? (
                        <Button
                            icon={{ icon: open ? CaretDown : CaretRight }}
                            variant="text"
                            data-testid={`file-preview-toggle-${name}`}
                            onClick={() => setOpen((visible) => !visible)}
                        />
                    ) : null
                }
            />
            {open && isImage && previewSrc && <PreviewImage src={previewSrc} alt={name} />}
            {open && isPdf && previewSrc && <PreviewFrame src={previewSrc} title={name} />}
            {open && isVideo && previewSrc && (
                <PreviewVideo controls preload="metadata">
                    <source src={previewSrc} type={fileType} />
                </PreviewVideo>
            )}
            {open && isText && text !== null && <PreviewText>{text}</PreviewText>}
        </FileCard>
    );
}

function ReadOnlyFileChip({ url, name }: { url: string; name: string }) {
    return (
        <InlineFileChip
            type="button"
            className="file-node"
            data-testid={`file-node-${name}`}
            onClick={() => {
                if (isSafeHref(url)) handleFileDownload(url, name);
            }}
        >
            {name}
        </InlineFileChip>
    );
}

const FONT_SIZE_VALUE =
    /^(?:0|[1-9]\d{0,3}(?:\.\d+)?(?:px|em|rem|%|pt)|smaller|larger|xx-small|x-small|small|medium|large|x-large|xx-large)$/i;

function fontSizeFromStyle(style: string): string | undefined {
    let fontSize: string | undefined;
    style.split(';').forEach((declaration) => {
        const separator = declaration.indexOf(':');
        if (separator < 0) return;
        const property = declaration.slice(0, separator).trim().toLowerCase();
        const value = declaration.slice(separator + 1).trim();
        if (property === 'font-size' && FONT_SIZE_VALUE.test(value)) fontSize = value;
    });
    return fontSize;
}

const URL_ATTRIBUTE_NAMES = new Set(['href', 'src', 'srcset', 'poster', 'action', 'formaction']);

function elementAttributes(element: HTMLElement): Record<string, unknown> {
    const props: Record<string, unknown> = {};
    Array.from(element.attributes).forEach((attr) => {
        const name = attr.name.toLowerCase();
        if (name.startsWith('on') || name === 'style') return;
        if (name === 'srcset') {
            const srcset = attr.value
                .split(',')
                .map((candidate) => candidate.trim())
                .filter(Boolean);
            const safe = srcset.filter((candidate) => isSafeHref(candidate.split(/\s+/)[0] ?? ''));
            if (safe.length === 0) return;
            props.srcSet = safe.length === srcset.length ? attr.value : safe.join(', ');
            return;
        }
        if (URL_ATTRIBUTE_NAMES.has(name)) {
            if (!isSafeHref(attr.value)) return;
            props[name === 'formaction' ? 'formAction' : name] = attr.value;
            return;
        }
        props[REACT_ATTR_NAMES[name] ?? attr.name] = attr.value;
    });
    const fontSize = fontSizeFromStyle(element.getAttribute('style') ?? '');
    if (fontSize) props.style = { fontSize };
    return props;
}

const TABLE_TAGS = new Set(['table', 'thead', 'tbody', 'tfoot', 'tr', 'colgroup']);

function fileCardProps(node: HTMLElement) {
    return {
        url: node.getAttribute(FILE_ATTRS.url) ?? '',
        name: node.getAttribute(FILE_ATTRS.name) ?? '',
        type: node.getAttribute(FILE_ATTRS.type) ?? '',
        size: node.getAttribute(FILE_ATTRS.size) ?? '',
        id: node.getAttribute(FILE_ATTRS.id) ?? '',
    };
}

function isFileNode(node: ChildNode): node is HTMLElement {
    return node instanceof HTMLElement && node.classList.contains('file-node');
}

const BLOCK_TAGS_WITH_PHRASING = new Set(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']);

function nodeToReact(node: ChildNode, key: string, parentTag?: string): React.ReactNode {
    if (node.nodeType === Node.TEXT_NODE) {
        const text = node.textContent ?? '';
        if (!text || (parentTag && TABLE_TAGS.has(parentTag) && !text.trim())) return null;
        return <React.Fragment key={key}>{text}</React.Fragment>;
    }
    if (!(node instanceof HTMLElement)) return null;

    if (node.classList.contains('file-node')) {
        if (parentTag && BLOCK_TAGS_WITH_PHRASING.has(parentTag)) {
            return (
                <ReadOnlyFileChip
                    key={key}
                    url={node.getAttribute(FILE_ATTRS.url) ?? ''}
                    name={node.getAttribute(FILE_ATTRS.name) ?? ''}
                />
            );
        }
        return <ReadOnlyFileCard key={key} {...fileCardProps(node)} />;
    }

    const tag = node.tagName.toLowerCase();
    const meaningfulChildren = Array.from(node.childNodes).filter(
        (child) => !(child.nodeType === Node.TEXT_NODE && !child.textContent?.trim()),
    );
    // A file card is a div. It can replace a paragraph or heading that contains nothing else.
    // A file mentioned in a sentence stays an inline chip so it is not a div inside that paragraph.
    if (BLOCK_TAGS_WITH_PHRASING.has(tag) && meaningfulChildren.length === 1 && isFileNode(meaningfulChildren[0])) {
        return <ReadOnlyFileCard key={key} {...fileCardProps(meaningfulChildren[0])} />;
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
