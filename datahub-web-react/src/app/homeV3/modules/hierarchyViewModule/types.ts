import { ASSET_TYPE_DOMAINS, ASSET_TYPE_GLOSSARY } from "./constants";

export type AssetType = typeof ASSET_TYPE_DOMAINS | typeof ASSET_TYPE_GLOSSARY;