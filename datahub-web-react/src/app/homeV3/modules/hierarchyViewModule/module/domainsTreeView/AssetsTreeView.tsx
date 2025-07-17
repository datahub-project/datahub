import React from 'react';
import { ASSET_TYPE_DOMAINS, ASSET_TYPE_GLOSSARY } from "../../constants";
import { AssetType } from "../../types";
import DomainsTreeView from "./DomainsTreeView";
import GlossaryTreeView from './GlossaryTreeVie';

interface Props {
    assetType: AssetType;
    assetUrns: string[];
}

export default function AssetsTreeView({assetType, assetUrns}:Props) {
    if (assetType === ASSET_TYPE_DOMAINS) {
        return <DomainsTreeView assetUrns={assetUrns} />;
    }

    if (assetType === ASSET_TYPE_GLOSSARY) {
        return <GlossaryTreeView assetUrns={assetUrns} />;
    }

    return null;
}