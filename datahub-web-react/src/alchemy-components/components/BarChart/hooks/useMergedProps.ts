/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useMemo } from 'react';

export default function useMergedProps<PropsType extends object>(
    props?: PropsType,
    defaultProps?: PropsType,
): PropsType {
    return useMemo(() => ({ ...(defaultProps || {}), ...(props || {}) }), [props, defaultProps]) as PropsType;
}
