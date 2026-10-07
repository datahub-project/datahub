import React from 'react';

/**
 * i18n key referenced by the OSS-only AIChat stub in the ingestion builder.
 * acryl-main deleted that component and passes MultiTabEmbeddedChat instead.
 *
 * Detected via function source so PageLayout can omit the rail without importing
 * or modifying AIChat.tsx — modifying that file would conflict with acryl's
 * deletion on automerge.
 */
const OSS_AI_CHAT_STUB_MARKER = 'multiStep.chat.placeholder';

/**
 * True when `rightPanelContent` is the OSS AIChat placeholder.
 * Returning null from that component is not enough: `<AIChat />` is still a
 * truthy element, and PageLayout would keep a 33% empty column.
 */
export function isHiddenRightPanel(content: React.ReactNode): boolean {
    if (!React.isValidElement(content)) return false;
    const { type } = content;
    if (typeof type !== 'function') return false;
    try {
        return Function.prototype.toString.call(type).includes(OSS_AI_CHAT_STUB_MARKER);
    } catch {
        return false;
    }
}
