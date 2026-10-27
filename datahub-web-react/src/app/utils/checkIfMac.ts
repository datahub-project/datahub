export function checkIfMac(): boolean {
    return (navigator as any).userAgentData
        ? (navigator as any).userAgentData.platform.toLowerCase().includes('mac')
        : navigator.userAgent.toLowerCase().includes('mac');
}

/** Label for the search-bar focus shortcut (⌘K on Mac, Ctrl+K elsewhere). */
export function getCommandKShortcutLabel(): string {
    return checkIfMac() ? '⌘K' : 'Ctrl+K';
}
