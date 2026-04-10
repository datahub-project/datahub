import React, { useState } from 'react';
import styled from 'styled-components';

import { getChangeEventString } from '@app/entityV2/shared/tabs/Dataset/Schema/history/changeEventToString';
import { processDocumentationString } from '@src/app/lineageV2/lineageUtils';

import { ChangeEvent } from '@types';

const MAX_DISPLAY_CHARS = 200;

const ChangeEventCircle = styled.div`
    display: inline-block;
    min-width: 8px;
    height: 8px;
    border-radius: 50%;
    border: 1px solid ${(props) => props.theme.colors.textTertiary};
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
}

const ChangeEventComponent: React.FC<ChangeTransactionProps> = ({ changeEvent, nameMap }) => {
    const [expanded, setExpanded] = useState(false);
    const fullString = getChangeEventString(changeEvent, nameMap);
    const needsTruncation = (fullString?.length ?? 0) > MAX_DISPLAY_CHARS;
    const displayString = needsTruncation && !expanded ? `${fullString?.slice(0, MAX_DISPLAY_CHARS)}...` : fullString;

    return (
        <ChangeEventContainer>
            <ChangeEventCircle />
            <ChangeEventText>
                {processDocumentationString(displayString)}
                {needsTruncation && (
                    <ToggleLink onClick={() => setExpanded((prev) => !prev)}>
                        {expanded ? 'Show less' : 'Show more'}
                    </ToggleLink>
                )}
            </ChangeEventText>
        </ChangeEventContainer>
    );
};

export default ChangeEventComponent;
