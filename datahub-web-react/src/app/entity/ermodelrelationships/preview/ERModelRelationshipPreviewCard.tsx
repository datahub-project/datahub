import React from 'react';
import { Card, Collapse } from 'antd';
import ermodelrelationshipIcon from '../../../../images/ermodelrelationshipIcon.svg';
import { EntityType, Owner, GlobalTags, GlossaryTerms } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { IconStyleType } from '../../Entity';

const { Panel } = Collapse;

export const ERModelRelationshipPreviewCard = ({
    urn,
    name,
    owners,
    description,
    globalTags,
    glossaryTerms,
}: {
    urn: string;
    name: string | any;
    description: string | any;
    globalTags?: GlobalTags | null;
    glossaryTerms?: GlossaryTerms | null;
    owners?: Array<Owner> | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const getERModelRelationHeader = (): JSX.Element => {
        return (
            <div style={{ width: '100%', display: 'inline-block' }}>
                <DefaultPreviewCard
                    url={entityRegistry.getEntityUrl(EntityType.ErModelRelationship, urn)}
                    name={name || ''}
                    urn={urn}
                    description={description || ''}
                    logoComponent={
                        <img src={ermodelrelationshipIcon} alt="ERModelRelationship" style={{ fontSize: '20px' }} />
                    }
                    tags={globalTags || undefined}
                    glossaryTerms={glossaryTerms || undefined}
                    owners={owners}
                    type="ERModelRelationship"
                    typeIcon={entityRegistry.getIcon(EntityType.ErModelRelationship, 14, IconStyleType.ACCENT)}
                    titleSizePx={18}
                />
            </div>
        );
    };

    return (
        <>
            <Card className="cardStyle" bordered key={`${urn}_1`}>
                <Panel header={getERModelRelationHeader()} key={`${urn}_2`} />
            </Card>
        </>
    );
};
