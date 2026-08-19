import { Popover, PopoverProps } from 'antd';
import * as React from 'react';

export default function DataHubPopover({ overlayInnerStyle, ...props }: PopoverProps & React.RefAttributes<unknown>) {
    // Merge (don't replace) the default font so callers that pass their own
    // overlayInnerStyle for padding/background/shadow still inherit Mulish instead of
    // silently falling back to antd's default font token (Manrope).
    return <Popover overlayInnerStyle={{ fontFamily: 'Mulish', ...overlayInnerStyle }} {...props} showArrow={false} />;
}
