import { Button, Loader, borders, colors, radius, spacing } from '@components';
import { useDraggable } from '@dnd-kit/core';
import React, { memo } from 'react';
import styled from 'styled-components';

import ModuleContainer from '@app/homeV3/module/components/ModuleContainer';
import ModuleMenu from '@app/homeV3/module/components/ModuleMenu';
import ModuleName from '@app/homeV3/module/components/ModuleName';
import { DragIcon } from '@app/homeV3/module/components/SmallModule';
import { ModuleProps } from '@app/homeV3/module/types';
import { FloatingRightHeaderSection } from '@app/homeV3/styledComponents';

const ModuleHeader = styled.div`
    position: relative;
    display: flex;
    flex-direction: column;
    gap: 2px;
    border-radius: ${radius.lg} ${radius.lg} 0 0;
    padding: ${spacing.sm} ${spacing.lg} ${spacing.sm} ${spacing.md};
    border-bottom: ${borders['1px']} ${colors.white};
    user-select: none;

    /* Optimize for smooth dragging */
    transform: translateZ(0);
    will-change: transform;

    :hover {
        background: linear-gradient(180deg, #fff 0%, #fafafb 100%);
        border-bottom: 1px solid ${colors.gray[100]};
    }

    :hover ${DragIcon} {
        display: block;
    }
`;

const DragHandle = styled.div<{ $isDragging?: boolean }>`
    cursor: ${({ $isDragging }) => ($isDragging ? 'grabbing' : 'grab')};
    flex: 1;
`;

const Content = styled.div<{ $hasViewAll: boolean }>`
    margin: 0 8px 8px 8px;
    overflow-y: auto;
    height: ${({ $hasViewAll }) => ($hasViewAll ? '226px' : '238px')};
`;

const LoaderContainer = styled.div`
    display: flex;
    height: 100%;
`;

const ViewAllButton = styled(Button)`
    margin-left: auto;
    margin-right: 16px;
    margin: 0 16px 0 auto;
`;

interface Props extends ModuleProps {
    loading?: boolean;
    onClickViewAll?: () => void;
}

function LargeModule({ children, module, position, loading, onClickViewAll }: React.PropsWithChildren<Props>) {
    const { name } = module.properties;

    const { attributes, listeners, setNodeRef, isDragging } = useDraggable({
        id: `module-${module.urn}-${position.rowIndex}-${position.moduleIndex}`,
        data: {
            module,
            position,
            isSmall: false,
        },
    });

    return (
        <ModuleContainer $height="316px" ref={setNodeRef}>
            <ModuleHeader>
                <DragHandle {...listeners} {...attributes} $isDragging={isDragging}>
                    <DragIcon
                        {...listeners}
                        size="lg"
                        color="gray"
                        icon="DotsSixVertical"
                        source="phosphor"
                        isDragging={isDragging}
                    />
                    <ModuleName text={name} />
                    {/* TODO: implement description for modules CH-548 */}
                    {/* <ModuleDescription text={description} /> */}
                </DragHandle>
                <FloatingRightHeaderSection>
                    <ModuleMenu module={module} position={position} />
                </FloatingRightHeaderSection>
            </ModuleHeader>
            <Content $hasViewAll={!!onClickViewAll}>
                {loading ? (
                    <LoaderContainer>
                        <Loader />
                    </LoaderContainer>
                ) : (
                    children
                )}
            </Content>
            {onClickViewAll && (
                <ViewAllButton variant="link" color="gray" size="sm" onClick={onClickViewAll} data-testid="view-all">
                    View all
                </ViewAllButton>
            )}
        </ModuleContainer>
    );
}

// Export memoized component to prevent unnecessary re-renders
export default memo(LargeModule);
