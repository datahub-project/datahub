import { Icon } from '@components';
import { useDraggable } from '@dnd-kit/core';
import React from 'react';
import styled from 'styled-components';

import ModuleContainer from '@app/homeV3/module/components/ModuleContainer';
import ModuleMenu from '@app/homeV3/module/components/ModuleMenu';
import { ModuleProps } from '@app/homeV3/module/types';
import { FloatingRightHeaderSection } from '@app/homeV3/styledComponents';

const DragIcon = styled(Icon)<{ isDragging: boolean }>`
    cursor: ${(props) => (props.isDragging ? 'grabbing' : 'grab')};
    display: none;
    position: absolute;
    left: 4px;
    top: 50%;
    transform: translateY(-50%);
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

const Content = styled.div`
    margin: 16px 32px 16px 16px;
    position: relative;
`;

const StyledModuleContainer = styled(ModuleContainer)<{ clickable?: boolean }>`
    max-height: 64px;

    ${({ clickable }) => clickable && `cursor: pointer;`}
`;

export default function SmallModule({ children, module, position, onClick }: React.PropsWithChildren<ModuleProps>) {
    const { attributes, listeners, setNodeRef, isDragging } = useDraggable({
        id: `module-${module.urn}-${position.rowIndex}-${position.moduleIndex}`,
        data: {
            module,
            position,
            isSmall: true,
        },
    });

    return (
        <StyledModuleContainer clickable={!!onClick} onClick={onClick} ref={setNodeRef} {...attributes}>
            <ContainerWithHover>
                <DragIcon
                    {...listeners}
                    size="lg"
                    color="gray"
                    icon="DotsSixVertical"
                    source="phosphor"
                    isDragging={isDragging}
                />
                <Content>{children}</Content>
                <FloatingRightHeaderSection>
                    <ModuleMenu module={module} position={position} />
                </FloatingRightHeaderSection>
            </ContainerWithHover>
        </StyledModuleContainer>
    );
}
