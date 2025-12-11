/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

const SchemaRow = React.forwardRef<HTMLTableRowElement>((props, ref) => {
    // eslint-disable-next-line react/prop-types
    const { children, ...rest } = props;
    return (
        <tr {...rest} ref={ref}>
            {children}
        </tr>
    );
});

export default SchemaRow;
