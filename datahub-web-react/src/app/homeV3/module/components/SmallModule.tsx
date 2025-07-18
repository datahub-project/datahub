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

const StyledModuleContainer = styled(ModuleContainer)`
    max-height: 72px;
`;

export default function SmallModule({ children, module, position }: React.PropsWithChildren<ModuleProps>) {
    return (
        <StyledModuleContainer>
            <Container>
                <Content>{children}</Content>
                <FloatingRightHeaderSection>
                    <ModuleMenu module={module} position={position} />
                </FloatingRightHeaderSection>
            </Container>
        </StyledModuleContainer>
    );
}
