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
