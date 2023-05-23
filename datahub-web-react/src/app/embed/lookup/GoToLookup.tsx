import React, { useEffect } from 'react';
import { useHistory } from 'react-router';
import { EntityType } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { PageRoutes } from '../../../conf/Global';
import { urlEncodeUrn } from '../../entity/shared/utils';
import LookupLoading from './LookupLoading';

const GoToLookup = ({ entityType, entityUrn }: { entityType: EntityType; entityUrn: string }) => {
    const registry = useEntityRegistry();
    const history = useHistory();
    useEffect(() => {
        const entityUrl = `${PageRoutes.EMBED}/${registry.getPathName(entityType)}/${urlEncodeUrn(entityUrn)}`;
        history.push(entityUrl);
    }, [entityType, entityUrn, history, registry]);
    return <LookupLoading />;
};

export default GoToLookup;
