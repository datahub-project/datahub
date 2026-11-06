import { Divider, Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { AvatarsGroup } from '@app/sharedV2/avatar';
import { safeUrl } from '@app/sharedV2/urlUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

const VerticalStack = styled.div`
    display: inline-flex;
    flex-direction: column;
    gap: 16px;
    margin-bottom: 15px;
`;

const SourceRow = styled.div`
    display: inline-flex;
    align-items: center;
    gap: 8px;
`;

const Paragraph = styled(Text).attrs({ type: 'p', size: 'inherit' })`
    line-height: inherit;
    margin-bottom: 1em;
`;

type Props = {
    definition: string;
    sourceRef: string;
    sourceUrl: string;
    ownership?: any;
};
export default function GlossaryTermHeader({ definition, sourceRef, sourceUrl, ownership }: Props) {
    const { t } = useTranslation('entity.types');
    const entityRegistry = useEntityRegistry();
    return (
        <>
            <VerticalStack>
                <Paragraph>{definition}</Paragraph>
                <SourceRow>
                    <Text type="span" size="inherit" style={{ lineHeight: 'inherit' }}>
                        {t('glossaryTerm.sourceLabel')}
                    </Text>
                    <Divider type="vertical" />
                    <Text type="span" size="inherit" style={{ lineHeight: 'inherit' }} weight="semiBold">
                        {sourceRef}
                    </Text>
                    {sourceUrl && (
                        <>
                            <Divider type="vertical" />
                            <a href={safeUrl(decodeURIComponent(sourceUrl))} target="_blank" rel="noreferrer">
                                {t('glossaryTerm.viewSource')}
                            </a>
                        </>
                    )}
                </SourceRow>
                <AvatarsGroup owners={ownership?.owners} entityRegistry={entityRegistry} />
            </VerticalStack>
        </>
    );
}
