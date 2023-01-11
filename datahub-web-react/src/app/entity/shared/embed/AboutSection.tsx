import { Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';
import MarkdownViewer, { MarkdownView } from '../components/legacy/MarkdownViewer';
import NoMarkdownViewer, { removeMarkdown } from '../components/styled/StripMarkdownText';
import { ANTD_GRAY } from '../constants';
import LinkButton from '../containers/profile/sidebar/LinkButton';
import { useEntityData } from '../EntityContext';

const ContentWrapper = styled.div`
    margin-bottom: 5px;
    font-size: 14px;
    ${MarkdownView} {
        font-size: 14px;
    }
`;

const NoContentWrapper = styled.div`
    color: ${ANTD_GRAY[7]};
    font-size: 12px;
`;

const ABBREVIATED_LIMIT = 150;

export default function AboutSection() {
    const { entityData } = useEntityData();
    const originalDescription = entityData?.properties?.description;
    const editedDescription = entityData?.editableProperties?.description;
    const description = editedDescription || originalDescription || '';
    const links = entityData?.institutionalMemory?.elements || [];

    const isOverLimit = removeMarkdown(description).length > ABBREVIATED_LIMIT;
    const [isExpanded, setIsExpanded] = useState(!isOverLimit);

    return (
        <div>
            <Typography.Title level={5}>About</Typography.Title>
            <ContentWrapper>
                {!!description && (
                    <>
                        {isExpanded && (
                            <>
                                <MarkdownViewer source={description} ignoreLimit />
                                {isOverLimit && (
                                    <Typography.Link onClick={() => setIsExpanded(false)}>Read Less</Typography.Link>
                                )}
                            </>
                        )}
                        {!isExpanded && (
                            <NoMarkdownViewer
                                limit={ABBREVIATED_LIMIT}
                                readMore={
                                    <Typography.Link onClick={() => setIsExpanded(true)}>Read More</Typography.Link>
                                }
                                shouldWrap
                            >
                                {description}
                            </NoMarkdownViewer>
                        )}
                    </>
                )}
                {!description && (
                    <NoContentWrapper>
                        No documentation yet. Share your knowledge by adding documentation and links to helpful
                        resources.
                    </NoContentWrapper>
                )}
            </ContentWrapper>
            {links.map((link) => (
                <LinkButton link={link} />
            ))}
        </div>
    );
}
