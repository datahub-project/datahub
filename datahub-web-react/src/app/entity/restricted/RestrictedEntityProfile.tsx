import React from 'react';
import styled from 'styled-components';
import {
    LogoIcon,
    PlatformContentWrapper,
    PlatformText,
    PreviewImage,
} from '../shared/containers/profile/header/PlatformContent/PlatformContentView';
import RestrictedIcon from '../../../images/restricted.svg';
import { EntityTitle } from '../shared/containers/profile/header/EntityName';

const SubHeader = styled.div`
    margin-top: 8px;
    font-size: 14px;
`;

export function RestrictedEntityProfile() {
    return (
        <>
            <PlatformContentWrapper>
                <LogoIcon>
                    <PreviewImage preview={false} src={RestrictedIcon} alt="restricted" />
                </LogoIcon>
                <PlatformText>Restrito</PlatformText>
            </PlatformContentWrapper>
            <EntityTitle level={3}>Ativo Restrito</EntityTitle>
            <SubHeader>Este ativo é restrito. Solicite acesso para ver mais.</SubHeader>
        </>
    );
}
