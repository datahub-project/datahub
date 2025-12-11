/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useContext } from 'react';

import { Theme } from '@conf/theme/types';

export const CustomThemeContext = React.createContext<{
    theme?: Theme;
    updateTheme: (theme: Theme) => void;
}>({ theme: undefined, updateTheme: (_) => null });

export function useCustomTheme() {
    return useContext(CustomThemeContext);
}
