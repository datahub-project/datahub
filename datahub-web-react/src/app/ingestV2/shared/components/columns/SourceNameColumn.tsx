import { Pill, Text, Tooltip, colors } from '@components';
import { Image, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import useGetSourceLogoUrl from '@app/ingestV2/source/builder/useGetSourceLogoUrl';
import { capitalizeFirstLetter } from '@app/shared/textUtil';

const NameContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 12px;
    width: 100%;
`;

const TextContainer = styled(Typography.Text)`
    color: ${colors.gray[1700]};
`;

const DisplayNameContainer = styled.div`
    display: flex;
    flex-direction: column;
    max-width: calc(100% - 50px);
`;

const TruncatedText = styled(Text)`
    max-width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const PreviewImage = styled(Image)`
    max-height: 28px;
    width: auto;
    object-fit: contain;
    margin: 0px;
    background-color: transparent;
`;

interface TypeColumnProps {
    type: string;
    record: any;
}

export function SourceNameColumn({ type, record }: TypeColumnProps) {
    const iconUrl = useGetSourceLogoUrl(type);
    const typeDisplayName = capitalizeFirstLetter(type);

    return (
        <NameContainer>
            {iconUrl && (
                <Tooltip overlay={typeDisplayName}>
                    <PreviewImage preview={false} src={iconUrl} alt={type || ''} />
                </Tooltip>
            )}
            <DisplayNameContainer>
                <TextContainer
                    ellipsis={{
                        tooltip: {
                            title: record.name,
                            color: 'white',
                            overlayInnerStyle: { color: colors.gray[1700] },
                            showArrow: false,
                        },
                    }}
                >
                    {record.name || ''}
                </TextContainer>
                {!iconUrl && typeDisplayName && <TruncatedText color="gray">{typeDisplayName}</TruncatedText>}
            </DisplayNameContainer>
            {record.cliIngestion && (
                <Tooltip title="This source is ingested from the command-line interface (CLI)">
                    <div>
                        <Pill label="CLI" color="blue" size="xs" />
                    </div>
                </Tooltip>
            )}
        </NameContainer>
    );
}
