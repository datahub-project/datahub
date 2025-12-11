/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

// these relationships trigger a link where the source is to the right of the sink
export const FORWARD_RELATIONSHIPS = ['DownstreamOf', 'Consumes', 'Contains', 'TrainedBy'];
// these relationships trigger a link where the source is to the left of the sink
export const INVERSE_RELATIONSHIPS = ['Produces', 'MemberOf'];

export const HORIZONTAL_SPACE_PER_LAYER = 400;
export const VERTICAL_SPACE_BETWEEN_NODES = 40;
export const EXPAND_COLLAPSE_COLUMNS_TOGGLE_HEIGHT = 35;
export const NUM_COLUMNS_PER_PAGE = 10;
export const COLUMN_HEIGHT = 30;

export const CURVE_PADDING = 75;

export const width = 250;
export const height = 80;
export const iconWidth = 32;
export const iconHeight = 32;
export const iconX = -width / 2 + 22;
export const iconY = -iconHeight / 2;
export const centerX = -width / 2;
export const centerY = -height / 2;
export const textX = iconX + iconWidth + 8;
export const healthX = -width / 2 + 14;
export const healthY = -iconHeight / 2 - 8;
