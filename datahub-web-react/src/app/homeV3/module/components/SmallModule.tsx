import React from 'react';
import styled from 'styled-components';

import ModuleContainer from '@app/homeV3/module/components/ModuleContainer';
import ModuleMenu from '@app/homeV3/module/components/ModuleMenu';
import { ModuleProps } from '@app/homeV3/module/types';
import { FloatingRightHeaderSection } from '@app/homeV3/styledComponents';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    position: relative;
    height: 100%;
    justify-content: center;
`;

const Content = styled.div`
    margin: 16px 32px 16px 16px;
    position: relative;
`;

const StyledModuleContainer = styled(ModuleContainer)<{ clickable?: boolean }>`
    max-height: 72px;

    ${({ clickable }) => clickable && `cursor: pointer;`}
`;

export default function SmallModule({ children, module, position, onClick }: React.PropsWithChildren<ModuleProps>) {
    return (
        <StyledModuleContainer clickable={!!onClick} onClick={onClick}>
            <Container>
                <Content>{children}</Content>
                <FloatingRightHeaderSection>
                    <ModuleMenu module={module} position={position} />
                </FloatingRightHeaderSection>
            </Container>
        </StyledModuleContainer>
    );
}
