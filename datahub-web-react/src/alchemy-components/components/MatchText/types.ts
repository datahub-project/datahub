/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { TextProps } from '@components/components/Text';

export type TextPropsWithoutChildren = Omit<TextProps, 'children'>;

export interface MatchTextProps extends TextPropsWithoutChildren {
    text: string;
    highlight: string;
    highlightedTextProps?: Partial<TextPropsWithoutChildren>;
}
