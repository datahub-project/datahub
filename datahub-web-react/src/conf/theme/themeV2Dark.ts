/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import dark from '@conf/theme/colorThemes/dark';
import themeV2 from '@conf/theme/themeV2';
import { Theme } from '@conf/theme/types';

const themeV2Dark: Theme = { ...themeV2, id: 'themeV2Dark', colors: dark };

export default themeV2Dark;
