import { Loader, borders, colors, radius, spacing } from '@components';
import React from 'react';
import styled from 'styled-components';

import ModuleContainer from '@app/homepageV2/module/components/ModuleContainer';
import ModuleDescription from '@app/homepageV2/module/components/ModuleDescription';
import ModuleMenu from '@app/homepageV2/module/components/ModuleMenu';
import ModuleName from '@app/homepageV2/module/components/ModuleName';
import PublicModuleBadge from '@app/homepageV2/module/components/PublicModuleBadge';
import { ModuleProps } from '@app/homepageV2/module/types';

const ModuleHeader = styled.div`
    position: relative;
    display: flex;
    flex-direction: column;
    gap: 2px;
    border-radius: ${radius.lg} ${radius.lg} 0 0;
    padding: ${spacing.md} ${spacing.md} ${spacing.xsm} ${spacing.md};
    border-bottom: ${borders['1px']} ${colors.white};

    :hover {
        background: linear-gradient(180deg, #fff 0%, #fafafb 100%);
        border-bottom: 1px solid ${colors.gray[100]};
    }
`;

const FloatingRightHeaderSection = styled.div`
    position: absolute;
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 8px;
    padding-right: 16px;
    right: 0px;
    top: 0px;
    height: 100%;
`;

const Content = styled.div`
    margin: 16px;
    overflow-y: auto;
    height: 222px;
`;

const LoaderContainer = styled.div`
    display: flex;
    height: 100%;
`;

interface Props extends ModuleProps {
    loading?: boolean;
}

export default function LargeModule({
    children,
    name,
    description,
    visibility,
    loading,
}: React.PropsWithChildren<Props>) {
    return (
        <ModuleContainer $height="316px">
            <ModuleHeader>
                <ModuleName text={name} />
                <ModuleDescription text={description} />
                <FloatingRightHeaderSection>
                    <PublicModuleBadge isPublic={visibility === 'global'} />
                    <ModuleMenu />
                </FloatingRightHeaderSection>
            </ModuleHeader>
            <Content>
                {loading ? (
                    <LoaderContainer>
                        <Loader />
                    </LoaderContainer>
                ) : (
                    children
                )}
            </Content>
        </ModuleContainer>
    );
}
