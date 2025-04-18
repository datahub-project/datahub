import React from 'react';
import { useGetLinkPreviewQuery } from '../../graphql/integration.generated';
import { InstitutionalMemoryMetadata, LinkPreviewType } from '../../types.generated';
import SlackMessageLinkPreview from './SlackMessageLinkPreview';

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
