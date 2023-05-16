import React, { useEffect } from 'react';
import { useHistory } from 'react-router';
import { EntityType } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import useEmbedLookupAnalytics from './useEmbedAnalytics';
import { PageRoutes } from '../../../conf/Global';
import { urlEncodeUrn } from '../../entity/shared/utils';
import LookupLoading from './LookupLoading';

const GoToLookup = ({ entityType, entityUrn }: { entityType: EntityType; entityUrn: string }) => {
    const registry = useEntityRegistry();
    const history = useHistory();
    const { trackLookupRoutingEvent } = useEmbedLookupAnalytics();
    const entityUrl = `${PageRoutes.EMBED}/${registry.getPathName(entityType)}/${urlEncodeUrn(entityUrn)}`;
    useEffect(() => {
        trackLookupRoutingEvent(entityType, entityUrn);
        history.push(entityUrl);
    }, [entityType, entityUrl, entityUrn, history, trackLookupRoutingEvent]);
    return <LookupLoading />;
};

export default GoToLookup;
