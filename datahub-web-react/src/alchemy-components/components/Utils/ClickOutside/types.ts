export type ClickOutsideCallback = (event: MouseEvent) => void;

export interface ClickOutsideOptions {
    wrappers?: React.RefObject<Element>[];
    outsideSelector?: string; // Selector for elements that should trigger `onClickOutside`
    ignoreSelector?: string; // Selector for elements that should be ignored
    ignoreWrapper?: boolean; // Enable to ignore click outside the wrapper
}

export interface ClickOutsideProps extends Omit<ClickOutsideOptions, 'wrappers'> {
    onClickOutside: ClickOutsideCallback;
    width?: string;
}
