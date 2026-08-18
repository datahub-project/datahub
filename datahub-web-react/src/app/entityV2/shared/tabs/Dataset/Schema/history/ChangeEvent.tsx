import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import DocumentationDiff from '@app/entityV2/shared/tabs/Dataset/Schema/history/DocumentationDiff';
import { PARAM_DESCRIPTION } from '@app/entityV2/shared/tabs/Dataset/Schema/history/HistorySidebar.utils';
import { getChangeEventString } from '@app/entityV2/shared/tabs/Dataset/Schema/history/changeEventToString';
import { processDocumentationString } from '@src/app/lineageV3/utils/lineageUtils';

import { ChangeCategoryType, ChangeEvent } from '@types';

const MAX_DISPLAY_CHARS = 200;

const ChangeEventCircle = styled.div`
    display: inline-block;
    min-width: 8px;
    height: 8px;
    border-radius: 50%;
    border: 1px solid ${(props) => props.theme.colors.border};
    margin-top: 8px;
`;

const ChangeEventText = styled.div`
    display: inline-block;
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 13px;
    font-style: normal;
    font-weight: 400;
    line-height: 20px;
    letter-spacing: -0.12px;
    margin-left: 22px;
    width: calc(100% - 22px);
`;

const ChangeEventContainer = styled.div`
    display: flex;
    flex-direction: row;
    width: 100%;
    margin-top: 8px;
    word-wrap: break-word;
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

interface ChangeTransactionProps {
    changeEvent: ChangeEvent;
    nameMap?: Map<string, string>;
    inheritedPreviousDescription?: string;
}

const ChangeEventComponent: React.FC<ChangeTransactionProps> = ({
    changeEvent,
    nameMap,
    inheritedPreviousDescription,
}) => {
    const { t: tc } = useTranslation('common.actions');
    const [expanded, setExpanded] = useState(false);

    // Documentation events with a description parameter get a diff view.
    const isDocWithParams =
        (changeEvent.category as string) === ChangeCategoryType.Documentation &&
        (changeEvent.parameters || []).some((p) => p.key === PARAM_DESCRIPTION);

    if (isDocWithParams) {
        return (
            <ChangeEventContainer data-testid="change-event-row">
                <ChangeEventCircle />
                <ChangeEventText>
                    <DocumentationDiff
                        changeEvent={changeEvent}
                        inheritedPreviousDescription={inheritedPreviousDescription}
                    />
                </ChangeEventText>
            </ChangeEventContainer>
        );
    }

    const fullString = getChangeEventString(changeEvent, nameMap);
    const needsTruncation = (fullString?.length ?? 0) > MAX_DISPLAY_CHARS;
    const displayString = needsTruncation && !expanded ? `${fullString?.slice(0, MAX_DISPLAY_CHARS)}...` : fullString;

    return (
        <ChangeEventContainer data-testid="change-event-row">
            <ChangeEventCircle />
            <ChangeEventText>
                {processDocumentationString(displayString)}
                {needsTruncation && (
                    <ToggleLink onClick={() => setExpanded((prev) => !prev)}>
                        {expanded ? tc('showLess') : tc('showMore')}
                    </ToggleLink>
                )}
            </ChangeEventText>
        </ChangeEventContainer>
    );
};

export default ChangeEventComponent;
