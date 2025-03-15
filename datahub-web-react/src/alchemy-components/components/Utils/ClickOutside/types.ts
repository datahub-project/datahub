export interface ClickOutsideProps {
    onClickOutside: (event: MouseEvent) => void;
    outsideSelector?: string; // Selector for elements that should trigger `onClickOutside`
    ignoreSelector?: string; // Selector for elements that should be ignored
    ignoreWrapper?: boolean; // Enable to ignore click outside the wrapper
}
