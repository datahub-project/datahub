/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { PageTitle } from '@components';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { useUserPersonaTitle } from '@app/homeV2/persona/useUserPersona';
import { getGreetingText } from '@app/homeV2/reference/header/getGreetingText';
import { useAppConfig } from '@app/useAppConfig';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { EntityType } from '@types';

const Container = styled.div`
    // FYI: horizontal 8px padding to align with the search bar's input as it has a wrapper on focus.
    // bottom 8px to add gap between the greeting text and the search bar. Flex gap breaks the views popover
    padding: 0 8px 8px 8px;
`;

export default function GreetingText() {
    const greetingText = getGreetingText();
    const { user } = useUserContext();
    const entityRegistry = useEntityRegistryV2();
    const maybeRole = useUserPersonaTitle();
    const {
        config: {
            featureFlags: { showHomepageUserRole },
        },
    } = useAppConfig();

    const finalText = useMemo(() => {
        if (!user) return `${greetingText}!`;
        return `${greetingText}, ${entityRegistry.getDisplayName(EntityType.CorpUser, user)}!`;
    }, [greetingText, user, entityRegistry]);

    return (
        <Container>
            <PageTitle title={finalText} subTitle={showHomepageUserRole ? maybeRole : null} />
        </Container>
    );
}
