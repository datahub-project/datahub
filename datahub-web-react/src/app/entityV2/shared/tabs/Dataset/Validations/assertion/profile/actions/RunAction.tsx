import React, { useState } from 'react';

import styled from 'styled-components';
import { RefreshOutlined } from '@mui/icons-material';

import { ActionItem } from './ActionItem';
import { Assertion, AssertionSourceType, Monitor } from '../../../../../../../../../types.generated';
import { RunAssertionModal } from '../../builder/steps/preview/RunAssertionModal';
import { isMonitorActive } from '../../../acrylUtils';
import { useAppConfig } from '../../../../../../../../useAppConfig';

const StyledRefresh = styled(RefreshOutlined)`
    && {
        font-size: 16px;
        display: flex;
        padding-left: 2px;
    }
`;

type Props = {
    assertion: Assertion;
    monitor?: Monitor;
    canEdit: boolean;
    refetch?: () => void;
    isExpandedView?: boolean;
};

export const RunAction = ({ assertion, monitor, canEdit, refetch, isExpandedView = false }: Props) => {
    const [showResult, setShowResult] = useState(false);
    const { config } = useAppConfig();
    const isRunAssertionsEnabled = config?.featureFlags?.runAssertionsEnabled;
    const isNonNative = assertion.info?.source?.type !== AssertionSourceType.Native;

    if (!isRunAssertionsEnabled || !monitor || isNonNative) {
        // 4 cases to hide the run assertion button entirely:
        //      1. Feature flag is disabled.
        //      2. Monitor is missing for assertion (no params).
        //      3. Assertion is not native (external), so it cannot be run by us.
        //      4. We have a reachable, local (non-remote) connection to the table in the datahub-executor.
        return null;
    }

    const isActive = isMonitorActive(monitor);
    const assertionUrn = assertion.urn;

    const onRunAssertion = () => {
        // Modal will run the assertion on open.
        setShowResult(true);
    };

    const onCloseModal = () => {
        setShowResult(false);
        setTimeout(() => refetch?.(), 2000);
    };

    const isActiveTip = !isActive ? 'This assertion is currently stopped! Start the assertion to run.' : undefined;
    const unauthorizedTip = canEdit ? 'Run this assertion' : 'You do not have permission to run this assertion';
    const runDisabled = !isActive || !canEdit;

    return (
        <>
            <ActionItem
                key="run-action"
                tip={!isActive ? isActiveTip : unauthorizedTip}
                disabled={runDisabled}
                onClick={onRunAssertion}
                icon={<StyledRefresh />}
                isExpandedView={isExpandedView}
                actionName="Run"
            />
            <RunAssertionModal urn={assertionUrn} visible={showResult} handleClose={onCloseModal} />
        </>
    );
};
