import { Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';
import CompactContext from '../../../../../../shared/CompactContext';
import MarkdownViewer, { MarkdownView } from '../../../../components/legacy/MarkdownViewer';
import NoMarkdownViewer, { removeMarkdown } from '../../../../components/styled/StripMarkdownText';
import { useRouteToTab } from '../../../../EntityContext';
import { useIsOnTab } from '../../utils';

const ABBREVIATED_LIMIT = 150;

const ContentWrapper = styled.div`
    margin-bottom: 8px;
    font-size: 14px;
    ${MarkdownView} {
        font-size: 14px;
    }
`;

interface Props {
    description: string;
}

export default function DescriptionSection({ description }: Props) {
    const isOverLimit = description && removeMarkdown(description).length > ABBREVIATED_LIMIT;
    const [isExpanded, setIsExpanded] = useState(!isOverLimit);
    const routeToTab = useRouteToTab();
    const isCompact = React.useContext(CompactContext);
    const shouldShowReadMore = !useIsOnTab('Documentation');

    // if we're not in compact mode, route them to the Docs tab for the best documentation viewing experience
    function readMore() {
        if (isCompact) {
            setIsExpanded(true);
        } else {
            routeToTab({ tabName: 'Documentation' });
        }
    }

    return (
        <ContentWrapper>
            {isExpanded && (
                <>
                    <MarkdownViewer source={description} ignoreLimit />
                    {isOverLimit && <Typography.Link onClick={() => setIsExpanded(false)}>Read Less</Typography.Link>}
                </>
            )}
            {!isExpanded && (
                <NoMarkdownViewer
                    limit={ABBREVIATED_LIMIT}
                    readMore={
                        shouldShowReadMore ? <Typography.Link onClick={readMore}>Read More</Typography.Link> : undefined
                    }
                    shouldWrap
                >
                    {description}
                </NoMarkdownViewer>
            )}
        </ContentWrapper>
    );
}
