import React from 'react';
import styled from 'styled-components';
import SharedByInfo from '@src/app/shared/share/items/MetadataShareItem/SharedByInfo';
import { useAppConfig } from '../../../../../useAppConfig';
import { useEntityData } from '../../../EntityContext';
import { SharedEntityInfo } from './SharedEntityInfo';

const Container = styled.div``;

const Heading = styled.div`
    font-weight: 600;
    margin-bottom: 4px;
`;

export const SidebarMetadataSection = () => {
    const appConfig = useAppConfig();
    const { entityData } = useEntityData();

    // Hide if flag disabled
    const { metadataShareEnabled } = appConfig.config.featureFlags;
    if (!metadataShareEnabled) return null;

    // Get latest share data
    const lastShareResults = entityData?.share?.lastShareResults;
    // Hide if no share result
    if (!lastShareResults || lastShareResults?.length === 0 || !lastShareResults[0]) return null;

    const filteredResults = lastShareResults?.filter((result) => !result.implicitShareEntity);

    const implicitShares = lastShareResults?.filter((result) => !!result.implicitShareEntity);

    return (
        <Container>
            {filteredResults && filteredResults.length > 0 && (
                <>
                    <Heading>Sharing with</Heading>
                    <SharedEntityInfo lastShareResults={filteredResults} />
                </>
            )}
            {implicitShares && implicitShares.length > 0 && (
                <>
                    <Heading>
                        Shared by &nbsp;
                        <SharedByInfo />
                    </Heading>
                    <SharedEntityInfo lastShareResults={implicitShares} isImplicitList showOnSidebar />
                </>
            )}
        </Container>
    );
};
