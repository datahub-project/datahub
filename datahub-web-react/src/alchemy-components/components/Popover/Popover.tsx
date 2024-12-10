import { Popover, PopoverProps } from 'antd';
import * as React from 'react';

export default function DataHubPopover(props: PopoverProps & React.RefAttributes<unknown>) {
    return <Popover {...props} showArrow={false} />;
}
