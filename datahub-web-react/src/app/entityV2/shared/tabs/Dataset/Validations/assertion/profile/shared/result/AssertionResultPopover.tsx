import { Popover } from '@components';
import { TooltipPlacement } from 'antd/lib/tooltip';
import React from 'react';

import { AssertionResultPopoverContent } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/result/AssertionResultPopoverContent';
import { ResultStatusType } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/resultMessageUtils';

import { Assertion, AssertionRunEvent, Maybe, Monitor } from '@types';

type Props = {
    assertion: Assertion;
    monitor?: Maybe<Monitor>;
    run?: AssertionRunEvent;
    showProfileButton?: boolean;
    onClickProfileButton?: () => void;
    placement?: TooltipPlacement;
    children: React.ReactNode;
    resultStatusType?: ResultStatusType;
    refetchResults?: () => Promise<unknown>;
    openAssertionNote?: () => void;
};

export const AssertionResultPopover = ({
    assertion,
    monitor,
    run,
    showProfileButton,
    onClickProfileButton,
    placement,
    children,
    resultStatusType,
    refetchResults,
    openAssertionNote,
}: Props) => {
    return (
        <Popover
            overlayInnerStyle={{ width: 400, overflow: 'hidden' }}
            showArrow={false}
            trigger="hover"
            content={
                <AssertionResultPopoverContent
                    assertion={assertion}
                    monitor={monitor}
                    run={run}
                    showProfileButton={showProfileButton}
                    onClickProfileButton={onClickProfileButton}
                    resultStatusType={resultStatusType}
                    refetchResults={refetchResults}
                    openAssertionNote={openAssertionNote}
                />
            }
            placement={placement}
        >
            {children}
        </Popover>
    );
};
