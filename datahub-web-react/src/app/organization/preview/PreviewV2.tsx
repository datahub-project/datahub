import React from 'react';
import { BankOutlined } from '@ant-design/icons';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { EntityType, Owner } from '../../../types.generated';
import { PreviewType } from '@app/entityV2/Entity';

interface Props {
    urn: string;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    logoComponent?: JSX.Element;
    previewType: PreviewType;
}

export const PreviewV2 = ({ urn, name, description, owners, logoComponent, previewType }: Props) => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Organization, urn)}
            name={name || ''}
            urn={urn}
            description={description || ''}
            entityType={EntityType.Organization}
            typeIcon={<BankOutlined style={{ fontSize: 14, color: '#BFBFBF' }} />}
            owners={owners}
            logoComponent={logoComponent}
            previewType={previewType}
        />
    );
};
