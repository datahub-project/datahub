import { Alert } from 'antd';
import { ArrowClockwise, Warning } from 'phosphor-react';
import React from 'react';
import styled from 'styled-components';

import { ValidationWarning as ValidationWarningType } from '@app/tests/builder/validation/utils';
import { Button, colors } from '@src/alchemy-components';

const StyledAlert = styled(Alert)`
    margin-bottom: 16px;
    border-radius: 6px;
    border: 1px solid ${colors.yellow[200]};
    background-color: ${colors.yellow[0]};

    .ant-alert-message {
        font-weight: 600;
        color: ${colors.gray[600]};
        font-size: 14px;
    }

    .ant-alert-description {
        color: ${colors.gray[600]};
        font-size: 14px;
        line-height: 1.4;
    }

    .ant-alert-icon {
        color: ${colors.yellow[600]};
    }
`;

const WarningList = styled.div`
    margin-top: 12px;

    .warning-item {
        margin-bottom: 6px;
        font-size: 13px;
        color: ${colors.gray[600]};
        line-height: 1.4;

        &::before {
            content: '•';
            color: ${colors.yellow[600]};
            margin-right: 8px;
            font-weight: bold;
        }
    }
`;

const ActionContainer = styled.div`
    margin-top: 16px;
    display: flex;
    gap: 8px;
    padding-top: 8px;
    border-top: 1px solid ${colors.gray[100]};
`;

interface Props {
    warnings: ValidationWarningType[];
    onResetFilters?: () => void;
    onResetActions?: () => void;
    showResetFilters?: boolean;
    showResetActions?: boolean;
}

export const ValidationWarning: React.FC<Props> = ({
    warnings,
    onResetFilters,
    onResetActions,
    showResetFilters = false,
    showResetActions = false,
}) => {
    if (warnings.length === 0) return null;

    const propertyWarnings = warnings.filter((w) => w.type === 'property');
    const actionWarnings = warnings.filter((w) => w.type === 'action');

    const hasPropertyWarnings = propertyWarnings.length > 0;
    const hasActionWarnings = actionWarnings.length > 0;

    const shouldShowResetFilters = showResetFilters && hasPropertyWarnings && onResetFilters;
    const shouldShowResetActions = showResetActions && hasActionWarnings && onResetActions;
    const shouldShowActionContainer = shouldShowResetFilters || shouldShowResetActions;

    return (
        <StyledAlert
            type="warning"
            icon={<Warning size={16} />}
            message="Invalid Configuration Detected"
            description={
                <div>
                    <div>
                        Some of your current selections are not compatible with the chosen entity types. This may cause
                        the test to fail when executed.
                    </div>

                    <WarningList>
                        {propertyWarnings.map((warning) => (
                            <div key={`property-${warning.propertyId || warning.message}`} className="warning-item">
                                {warning.message}
                            </div>
                        ))}
                        {actionWarnings.map((warning) => (
                            <div key={`action-${warning.actionId || warning.message}`} className="warning-item">
                                {warning.message}
                            </div>
                        ))}
                    </WarningList>

                    {shouldShowActionContainer ? (
                        <ActionContainer>
                            {shouldShowResetFilters ? (
                                <Button size="sm" variant="outline" onClick={onResetFilters}>
                                    <ArrowClockwise size={14} />
                                    Reset Filters
                                </Button>
                            ) : null}
                            {shouldShowResetActions ? (
                                <Button size="sm" variant="outline" onClick={onResetActions}>
                                    <ArrowClockwise size={14} />
                                    Reset Actions
                                </Button>
                            ) : null}
                        </ActionContainer>
                    ) : null}
                </div>
            }
        />
    );
};
