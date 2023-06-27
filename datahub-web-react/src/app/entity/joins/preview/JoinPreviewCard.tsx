import React from 'react';
import { Card, Collapse } from 'antd';
import joinIcon from '../../../../images/joinIcon.svg';
import { EntityType, Owner, GlobalTags, GlossaryTerms } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { IconStyleType } from '../../Entity';

const { Panel } = Collapse;

export const JoinPreviewCard = ({
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
    const getJoinHeader = (): JSX.Element => {
        return (
            <div style={{ width: '100%', display: 'inline-block' }}>
                <DefaultPreviewCard
                    url={entityRegistry.getEntityUrl(EntityType.Join, urn)}
                    name={name || ''}
                    urn={urn}
                    description={description || ''}
                    logoComponent={<img src={joinIcon} alt="Join" style={{ fontSize: '20px' }} />}
                    tags={globalTags || undefined}
                    glossaryTerms={glossaryTerms || undefined}
                    owners={owners}
                    type="Join"
                    typeIcon={entityRegistry.getIcon(EntityType.Join, 14, IconStyleType.ACCENT)}
                    titleSizePx={18}
                />
            </div>
        );
    };

    return (
        <>
            <Card className="cardStyle" bordered key={`${urn}_1`}>
                <Panel header={getJoinHeader()} key={`${urn}_2`} />
            </Card>
        </>
    );
};
