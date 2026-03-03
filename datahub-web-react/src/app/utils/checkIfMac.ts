export function checkIfMac(): boolean {
    return (navigator as any).userAgentData
        ? (navigator as any).userAgentData.platform.toLowerCase().includes('mac')
        : navigator.userAgent.toLowerCase().includes('mac');
}
