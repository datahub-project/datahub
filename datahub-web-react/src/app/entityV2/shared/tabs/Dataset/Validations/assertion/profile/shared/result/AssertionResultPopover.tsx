import React from 'react';

import { Popover } from '@components';
import { TooltipPlacement } from 'antd/lib/tooltip';

import { Assertion, AssertionRunEvent } from '../../../../../../../../../../types.generated';
import { AssertionResultPopoverContent } from './AssertionResultPopoverContent';
import { ResultStatusType } from '../../summary/shared/resultMessageUtils';

type Props = {
    assertion: Assertion;
    run?: AssertionRunEvent;
    showProfileButton?: boolean;
    onClickProfileButton?: () => void;
    placement?: TooltipPlacement;
    children: React.ReactNode;
    resultStatusType?: ResultStatusType;
};

export const AssertionResultPopover = ({
    assertion,
    run,
    showProfileButton,
    onClickProfileButton,
    placement,
    children,
    resultStatusType,
}: Props) => {
    return (
        <Popover
            overlayInnerStyle={{ width: 400, overflow: 'hidden' }}
            showArrow={false}
            trigger="hover"
            content={
                <AssertionResultPopoverContent
                    assertion={assertion}
                    run={run}
                    showProfileButton={showProfileButton}
                    onClickProfileButton={onClickProfileButton}
                    resultStatusType={resultStatusType}
                />
            }
            placement={placement}
        >
            {children}
        </Popover>
    );
};
