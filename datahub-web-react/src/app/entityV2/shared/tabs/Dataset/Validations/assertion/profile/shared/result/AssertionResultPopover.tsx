/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Popover } from '@components';
import { TooltipPlacement } from 'antd/lib/tooltip';
import React from 'react';

import { AssertionResultPopoverContent } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/result/AssertionResultPopoverContent';
import { ResultStatusType } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/resultMessageUtils';

import { Assertion, AssertionRunEvent } from '@types';

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
