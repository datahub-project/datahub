import { EditOutlined, FileOutlined } from '@ant-design/icons';
import React, { useCallback, useState } from 'react';
import styled from 'styled-components';

import { useEntityData, useRefetch, useRouteToTab } from '@app/entity/shared/EntityContext';
import { AddLinkModal } from '@app/entityV2/shared/components/styled/AddLinkModal';
import { EmptyTab } from '@app/entityV2/shared/components/styled/EmptyTab';
import { SectionContainer, SummaryTabHeaderTitle } from '@app/entityV2/shared/summary/HeaderComponents';
import { LinkList } from '@app/entityV2/shared/tabs/Documentation/components/LinkList';
import { Editor } from '@app/entityV2/shared/tabs/Documentation/components/editor/Editor';
import { Button } from '@src/alchemy-components';

const UNEXPANDED_HEIGHT = 2000;

const DocumentationWrapper = styled.div<{ canExpand?: boolean }>`
    position: relative;

    && {
        margin: 0;
        ${({ canExpand }) => canExpand && 'margin-bottom: 50px;'}
    }

    && .ant-empty {
        padding: 0;
    }

    .remirror-editor.ProseMirror {
        padding: 0px 8px;
    }
`;

const EditorWrapper = styled.div<{ mask?: boolean; maxHeight: string }>`
    max-height: ${({ maxHeight }) => maxHeight};
    overflow-y: hidden;
    ${({ mask }) =>
        mask &&
        `-webkit-mask-image: linear-gradient(to bottom, rgba(0,0,0,1) 50%, rgba(255,0,0,0.5) 60%, rgba(255,0,0,0) 90% );
         mask-image: linear-gradient(to bottom, rgba(0,0,0,1) 80%, rgba(255,0,0,0.5) 95%, rgba(255,0,0,0) 100%);`}
`;

const ExpandButton = styled(Button)`
    position: absolute;
    left: 50%;
    top: ${UNEXPANDED_HEIGHT + 10}px;
    transform: translateX(-50%);
    z-index: 1;
`;

export default function SummaryAboutSection() {
    const { entityData } = useEntityData();
    const refetch = useRefetch();
    const routeToTab = useRouteToTab();

    const [height, setHeight] = useState(0);
    const measuredRef = useCallback((node) => {
        if (node !== null) {
            const resizeObserver = new ResizeObserver(() => {
                setHeight(node.getBoundingClientRect().height);
            });
            resizeObserver.observe(node);
        }
    }, []);
    const [expanded, setExpanded] = useState(false);
    const maxHeight = expanded ? 'unset' : `${UNEXPANDED_HEIGHT}px`;
    const description = entityData?.editableProperties?.description || entityData?.properties?.description || '';
    const canExpand = !!description && !expanded && height >= UNEXPANDED_HEIGHT;

    return (
        <SectionContainer>
            <SummaryTabHeaderTitle title="Documentation" icon={<FileOutlined />} />
            <DocumentationWrapper canExpand={canExpand ? true : undefined}>
                {!!description && (
                    <>
                        <EditorWrapper ref={measuredRef} mask={canExpand ? true : undefined} maxHeight={maxHeight}>
                            <Editor content={description} readOnly />
                        </EditorWrapper>
                        {canExpand && <ExpandButton onClick={() => setExpanded(true)}>Expand</ExpandButton>}
                    </>
                )}
                {!description && (
                    <EmptyTab tab="documentation" hideImage>
                        <AddLinkModal refetch={refetch} />
                        <Button
                            data-testid="add-documentation"
                            onClick={() => routeToTab({ tabName: 'Documentation', tabParams: { editing: true } })}
                        >
                            <EditOutlined /> Add Documentation
                        </Button>
                    </EmptyTab>
                )}
                <LinkList refetch={refetch} />
            </DocumentationWrapper>
        </SectionContainer>
    );
}
