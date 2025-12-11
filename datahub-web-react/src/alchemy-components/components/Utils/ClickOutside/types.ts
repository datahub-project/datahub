/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
