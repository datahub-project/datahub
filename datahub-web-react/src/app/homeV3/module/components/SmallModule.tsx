import { Icon } from '@components';
import { useDraggable } from '@dnd-kit/core';
import React from 'react';
import styled from 'styled-components';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import ModuleContainer from '@app/homeV3/module/components/ModuleContainer';
import ModuleMenu from '@app/homeV3/module/components/ModuleMenu';
import { ModuleProps } from '@app/homeV3/module/types';
import { FloatingRightHeaderSection } from '@app/homeV3/styledComponents';

export const DragIcon = styled(Icon)<{ isDragging: boolean }>`
    cursor: ${(props) => (props.isDragging ? 'grabbing' : 'grab')};
    display: none;
    position: absolute;
    left: 0px;
    top: 50%;
    transform: translateY(-50%);
    height: 80%;
`;

const ContainerWithHover = styled.div`
    display: flex;
    flex-direction: column;
    position: relative;
    height: 100%;
    justify-content: center;

    :hover {
        background: linear-gradient(180deg, #fff 0%, #fafafb 100%);
    }

    :hover ${DragIcon} {
        display: block;
    }
`;

const Content = styled.div<{ $clickable?: boolean }>`
    margin: 16px 32px 16px 16px;
    position: relative;

    ${({ $clickable }) => $clickable && `cursor: pointer;`}
`;

const StyledModuleContainer = styled(ModuleContainer)`
    max-height: 64px;
`;

interface Props extends ModuleProps {
    dataTestId?: string;
}

export default function SmallModule({
    children,
    module,
    position,
    onClick,
    dataTestId,
}: React.PropsWithChildren<Props>) {
    const { isTemplateEditable } = usePageTemplateContext();
    const { attributes, listeners, setNodeRef, isDragging } = useDraggable({
        id: `module-${module.urn}-${position.rowIndex}-${position.moduleIndex}`,
        data: {
            module,
            position,
            isSmall: true,
        },
    });

    return (
        <StyledModuleContainer ref={setNodeRef} {...attributes} data-testId={dataTestId}>
            <ContainerWithHover>
                {isTemplateEditable && (
                    <DragIcon
                        {...listeners}
                        size="lg"
                        color="gray"
                        icon="DotsSixVertical"
                        source="phosphor"
                        isDragging={isDragging}
                    />
                )}
                <Content $clickable={!!onClick} onClick={onClick}>
                    {children}
                </Content>
                {isTemplateEditable && (
                    <FloatingRightHeaderSection>
                        <ModuleMenu module={module} position={position} />
                    </FloatingRightHeaderSection>
                )}
            </ContainerWithHover>
        </StyledModuleContainer>
    );
}
