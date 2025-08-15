import { Typography } from 'antd';
import DOMPurify from 'dompurify';
import React, { useState } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components/macro';

import { useRouteToTab } from '@app/entity/shared/EntityContext';
import MarkdownViewer, { MarkdownView } from '@app/entity/shared/components/legacy/MarkdownViewer';
import NoMarkdownViewer, { removeMarkdown } from '@app/entity/shared/components/styled/StripMarkdownText';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { useIsOnTab } from '@app/entity/shared/containers/profile/utils';
import CompactContext from '@app/shared/CompactContext';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

const ABBREVIATED_LIMIT = 150;

const ContentWrapper = styled.div`
    margin-bottom: 8px;
    font-size: 14px;
    ${MarkdownView} {
        font-size: 14px;
    }
`;

const BaContentWrapper = styled.div`
    margin-top: 8px;
    color: ${ANTD_GRAY[7]};
    margin-bottom: 8px;
    font-size: 14px;
    ${MarkdownView} {
        font-size: 14px;
    }
    color: ${ANTD_GRAY[7]};
`;

interface Props {
    description: string;
    baDescription?: string;
    isExpandable?: boolean;
    limit?: number;
    baUrn?: string;
}

export default function DescriptionSection({ description, baDescription, isExpandable, limit, baUrn }: Props) {
    const history = useHistory();
    const isOverLimit = description && removeMarkdown(description).length > ABBREVIATED_LIMIT;
    const isBaOverLimit = baDescription && removeMarkdown(baDescription).length > ABBREVIATED_LIMIT;
    const [isExpanded, setIsExpanded] = useState(!isOverLimit);
    const [isBaExpanded, setIsBaExpanded] = useState(!isBaOverLimit);
    const routeToTab = useRouteToTab();
    const isCompact = React.useContext(CompactContext);
    const shouldShowReadMore = !useIsOnTab('Documentation') || isExpandable;
    const entityRegistry = useEntityRegistry();

    // if we're not in compact mode, route them to the Docs tab for the best documentation viewing experience
    function readMore() {
        if (isCompact || isExpandable) {
            setIsExpanded(true);
        } else {
            routeToTab({ tabName: 'Documentation' });
        }
    }

    function readBAMore() {
        if (isCompact || isExpandable) {
            setIsBaExpanded(true);
        } else if (baUrn != null) {
            history.push(entityRegistry.getEntityUrl(EntityType.BusinessAttribute, baUrn || ''));
        }
    }

    const sanitizedDescription = DOMPurify.sanitize(description);
    const sanitizedBADescription = DOMPurify.sanitize(baDescription || '');

    return (
        <>
            <ContentWrapper>
                {isExpanded && (
                    <>
                        <MarkdownViewer source={sanitizedDescription} ignoreLimit />
                        {isOverLimit && (
                            <Typography.Link onClick={() => setIsExpanded(false)}>Read Less</Typography.Link>
                        )}
                    </>
                )}
                {!isExpanded && (
                    <NoMarkdownViewer
                        limit={limit || ABBREVIATED_LIMIT}
                        readMore={
                            shouldShowReadMore ? (
                                <Typography.Link onClick={readMore}>Read More</Typography.Link>
                            ) : undefined
                        }
                        shouldWrap
                    >
                        {sanitizedDescription}
                    </NoMarkdownViewer>
                )}
            </ContentWrapper>
            <BaContentWrapper>
                {isBaExpanded && (
                    <>
                        <MarkdownViewer source={sanitizedBADescription || ''} ignoreLimit />
                        {isBaOverLimit && (
                            <Typography.Link onClick={() => setIsBaExpanded(false)}>Read Less</Typography.Link>
                        )}
                    </>
                )}
                {!isBaExpanded && (
                    <NoMarkdownViewer
                        limit={limit || ABBREVIATED_LIMIT}
                        readMore={
                            shouldShowReadMore ? (
                                <Typography.Link onClick={readBAMore}>Read More</Typography.Link>
                            ) : undefined
                        }
                        shouldWrap
                    >
                        {sanitizedBADescription}
                    </NoMarkdownViewer>
                )}
            </BaContentWrapper>
        </>
    );
}
