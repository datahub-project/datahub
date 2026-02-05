export interface ResizablePillsProps<T> {
    // Core
    items: T[];
    renderPill: (item: T, index: number) => React.ReactNode;
    getItemWidth: (item: T) => number;

    // Overflow handling (handled inside component)
    overflowTooltipContent?: (hiddenItems: T[]) => React.ReactNode;
    overflowLabel?: (count: number) => string;

    // Customization
    gap?: number;
    overflowButtonWidth?: number;
    minContainerWidthForOne?: number;

    // Styling
    className?: string;
    style?: React.CSSProperties;

    // Advanced
    keyExtractor?: (item: T) => string;
    debounceMs?: number;
}
