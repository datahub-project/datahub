import React from 'react';

import SlackMessageLinkPreview from '@app/integration/SlackMessageLinkPreview';

import { useGetLinkPreviewQuery } from '@graphql/integration.generated';
import { InstitutionalMemoryMetadata, LinkPreviewType } from '@types';

interface Props {
    link: InstitutionalMemoryMetadata;
}

export default function LinkPreview({ link }: Props) {
    const { data } = useGetLinkPreviewQuery({
        variables: {
            input: {
                url: link.url,
            },
        },
        fetchPolicy: 'cache-first',
    });

    const previewType = data?.getLinkPreview?.type;

    return (
        <>
            {previewType === LinkPreviewType.SlackMessage && (
                <SlackMessageLinkPreview url={link.url} preview={data?.getLinkPreview as any} />
            )}
        </>
    );
}
