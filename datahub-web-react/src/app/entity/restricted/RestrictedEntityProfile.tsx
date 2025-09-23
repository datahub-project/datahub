import React from 'react';
import styled from 'styled-components';

import { EntityTitle } from '@app/entity/shared/containers/profile/header/EntityName';
import {
    LogoIcon,
    PlatformContentWrapper,
    PlatformText,
    PreviewImage,
} from '@app/entity/shared/containers/profile/header/PlatformContent/PlatformContentView';

import RestrictedIcon from '@images/restricted.svg';

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
                <PlatformText>Restricted</PlatformText>
            </PlatformContentWrapper>
            <EntityTitle level={3}>Restricted Asset</EntityTitle>
            <SubHeader>This asset is Restricted. Please request access to see more.</SubHeader>
        </>
    );
}
