import { useCallback } from 'react';

import { useModulesContext } from '@app/homeV3/module/context/ModulesContext';

import { ActionRequest, ActionRequestType, DataHubPageModuleType } from '@types';

const DELAY_MS = 3000;

export type MinimalActionRequest = Pick<ActionRequest, 'urn' | 'type' | 'params'>;

export function useReloadModuleOnProposalApprove() {
    const { reloadModules } = useModulesContext();

    const getModuleTypesToReload = useCallback((actionRequest: MinimalActionRequest) => {
        if (
            actionRequest.type === ActionRequestType.CreateGlossaryNode &&
            !!actionRequest.params?.createGlossaryNodeProposal?.glossaryNode?.parentNode
        ) {
            // To reload contents module on glossary node's summary tab
            return [DataHubPageModuleType.ChildHierarchy];
        }

        if (
            actionRequest.type === ActionRequestType.CreateGlossaryTerm &&
            !!actionRequest.params?.createGlossaryTermProposal?.glossaryTerm?.parentNode
        ) {
            // To reload contents module on glossary node's summary tab
            return [DataHubPageModuleType.ChildHierarchy];
        }

        if (actionRequest.type === ActionRequestType.DomainAssociation) {
            // To reload assets module on domain's summary tab
            return [DataHubPageModuleType.Assets];
        }

        if (actionRequest.type === ActionRequestType.OwnerAssociation) {
            // To reload your assets module on home page
            return [DataHubPageModuleType.OwnedAssets];
        }

        if (actionRequest.type === ActionRequestType.TermAssociation) {
            // To reload assets module on term's summary tab
            return [DataHubPageModuleType.Assets];
        }

        return [];
    }, []);

    const onProposalApproved = useCallback(
        (actionRequests: MinimalActionRequest[]) => {
            // Reload modules
            const moduleTypesToReload = Array.from(new Set(actionRequests.map(getModuleTypesToReload).flat()));

            if (moduleTypesToReload.length) {
                reloadModules(moduleTypesToReload, DELAY_MS);
            }
        },
        [reloadModules, getModuleTypesToReload],
    );

    return onProposalApproved;
}
