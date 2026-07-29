import React, { useState } from 'react';
import ReactDiffViewer, { DiffMethod } from 'react-diff-viewer';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import {
    PARAM_DESCRIPTION,
    PARAM_PREVIOUS_DESCRIPTION,
} from '@app/entityV2/shared/tabs/Dataset/Schema/history/HistorySidebar.utils';
import { colors } from '@src/alchemy-components';

import { ChangeEvent, ChangeOperationType } from '@types';

// Strip surrounding triple-backtick fences so the diff sees clean text.
function stripCodeFences(text: string): string {
    return text.replace(/^```[a-z]*\n?/i, '').replace(/\n?```\s*$/, '');
}

function getParameter(parameters?: Array<{ key?: string | null; value?: string | null }> | null, key?: string): string {
    return parameters?.find((p) => p.key === key)?.value ?? '';
}

const DiffContainer = styled.div`
    margin-top: 6px;
    max-height: 320px;
    overflow-y: auto;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 4px;
    font-size: 12px;

    /* tighten up the react-diff-viewer table */
    table {
        width: 100%;
    }
    pre {
        font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
        font-size: 12px;
        line-height: 1.5;
        white-space: pre-wrap;
        word-break: break-word;
    }
`;

const ToggleLink = styled.span`
    color: ${(props) => props.theme.colors.hyperlinks};
    cursor: pointer;
    font-size: 12px;
    margin-left: 4px;
    &:hover {
        text-decoration: underline;
    }
`;

const SummaryLine = styled.span`
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 13px;
`;

interface Props {
    changeEvent: ChangeEvent;
    /** Previous description inherited from the prior version, used for ADD events in all-versions mode. */
    inheritedPreviousDescription?: string;
}

const diffViewerStyles = {
    diffContainer: {
        fontSize: '12px',
    },
    wordAdded: {
        background: colors.green[1300],
        color: colors.green[1000],
    },
    wordRemoved: {
        background: colors.red[100],
        color: colors.red[1000],
    },
};

function DocumentationDiff({ changeEvent, inheritedPreviousDescription }: Props) {
    const { t: tc } = useTranslation('common.actions');
    const [expanded, setExpanded] = useState(false);

    const newText = stripCodeFences(getParameter(changeEvent.parameters, PARAM_DESCRIPTION));
    const prevText = stripCodeFences(getParameter(changeEvent.parameters, PARAM_PREVIOUS_DESCRIPTION));

    const op = changeEvent.operation;
    const isModify = op === ChangeOperationType.Modify && prevText !== '';
    const isAdd = op === ChangeOperationType.Add;
    const isRemove = op === ChangeOperationType.Remove;

    // For ADD and REMOVE without a previous value, fall back to old text-based display.
    const hasDiff = isModify || isAdd || isRemove;
    if (!hasDiff) return null;

    // ADD in all-versions mode: use the prior version's last known description so the diff
    // shows what changed between versions instead of an all-green "everything added" view.
    const inheritedPrev = inheritedPreviousDescription ? stripCodeFences(inheritedPreviousDescription) : '';
    const oldValue = isAdd ? inheritedPrev : prevText || newText;
    const newValue = isRemove ? '' : newText || prevText;

    let summaryKey: string;
    if (isAdd) summaryKey = 'documentationAdded';
    else if (isRemove) summaryKey = 'documentationRemoved';
    else summaryKey = 'documentationUpdated';

    const summary = tc(summaryKey);

    return (
        <>
            <SummaryLine>{summary}</SummaryLine>
            <ToggleLink onClick={() => setExpanded((prev) => !prev)}>
                {expanded ? tc('hideDiff') : tc('showDiff')}
            </ToggleLink>
            {expanded && (
                <DiffContainer data-testid="documentation-diff-content">
                    <ReactDiffViewer
                        oldValue={oldValue}
                        newValue={newValue}
                        compareMethod={DiffMethod.LINES}
                        splitView={false}
                        hideLineNumbers
                        styles={diffViewerStyles}
                    />
                </DiffContainer>
            )}
        </>
    );
}

export default DocumentationDiff;
