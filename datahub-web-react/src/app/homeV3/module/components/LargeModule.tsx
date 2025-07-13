import { Button, Loader, borders, colors, radius, spacing } from '@components';
import React from 'react';
import styled from 'styled-components';

import ModuleContainer from '@app/homeV3/module/components/ModuleContainer';
import ModuleMenu from '@app/homeV3/module/components/ModuleMenu';
import ModuleName from '@app/homeV3/module/components/ModuleName';
import { ModuleProps } from '@app/homeV3/module/types';

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

const Content = styled.div<{ $hasViewAll: boolean }>`
    margin: 16px;
    overflow-y: auto;
    height: ${({ $hasViewAll }) => ($hasViewAll ? '210px' : '222px')};
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

export default function LargeModule({ children, module, position, loading, onClickViewAll }: React.PropsWithChildren<Props>) {
    const { name } = module.properties;
    return (
        <ModuleContainer $height="316px">
            <ModuleHeader>
                <ModuleName text={name} />
                {/* TODO: implement description for modules CH-548 */}
                {/* <ModuleDescription text={description} /> */}
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
