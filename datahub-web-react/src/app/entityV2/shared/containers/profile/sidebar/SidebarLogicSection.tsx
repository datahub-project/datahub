import { Button, CodeBlock } from '@components';
import React, { useContext, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { DBT_URN } from '@app/ingest/source/builder/constants';
import { useIsEmbeddedProfile } from '@app/shared/useEmbeddedProfileLinkProps';
import EntitySidebarContext from '@app/sharedV2/EntitySidebarContext';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { GetDataJobQuery } from '@src/graphql/dataJob.generated';

import { GetDatasetQuery } from '@graphql/dataset.generated';
import { EntityType, QueryEntity } from '@types';

const DEFAULT_LANGUAGE = 'sql';
const SOURCE_OPTION = 'source';
const FORMATTED_OPTION = 'formatted';

export function SidebarDatasetViewDefinitionSection() {
    const { t } = useTranslation('entity.shared.containers');
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const statement = baseEntity?.dataset?.viewProperties?.logic;
    const entityRegistry = useEntityRegistry();
    const externalUrl = entityRegistry.getEntityUrl(EntityType.Dataset, baseEntity?.dataset?.urn || '');
    if (!statement) return null;

    return (
        <SidebarLogicSection
            title={t('sidebar.logic.viewDefinitionTitle')}
            statement={statement}
            externalUrl={externalUrl}
        />
    );
}

export function SidebarDataJobTransformationLogicSection() {
    const { t } = useTranslation('entity.shared.containers');
    const baseEntity = useBaseEntity<GetDataJobQuery>();
    const statement = baseEntity?.dataJob?.dataTransformLogic?.transforms?.[0]?.queryStatement?.value;
    const entityRegistry = useEntityRegistry();
    const externalUrl = entityRegistry.getEntityUrl(EntityType.DataJob, baseEntity?.dataJob?.urn || '');
    if (!statement) return null;

    return (
        <SidebarLogicSection title={t('sidebar.logic.logicTitle')} statement={statement} externalUrl={externalUrl} />
    );
}

export function SidebarQueryLogicSection() {
    const { t } = useTranslation('entity.shared.containers');
    const baseEntity = useBaseEntity<{ entity: QueryEntity }>();
    const statement = baseEntity?.entity?.properties?.statement?.value;
    const entityRegistry = useEntityRegistry();
    const externalUrl = entityRegistry.getEntityUrl(EntityType.Query, baseEntity?.entity?.urn || '');
    const { fineGrainedOperations } = useContext(EntitySidebarContext);
    const highlightedStrings = useMemo(
        () => fineGrainedOperations?.map((e) => e.transformOperation)?.filter((s): s is string => !!s),
        [fineGrainedOperations],
    );

    if (!statement) return null;

    return (
        <SidebarLogicSection
            title={t('sidebar.logic.logicTitle')}
            statement={statement}
            highlightedStrings={highlightedStrings}
            externalUrl={externalUrl}
        />
    );
}

interface HelperProps {
    title: string;
    statement: string;
    highlightedStrings?: string[];
    externalUrl: string;
}

// exported for testing only
function SidebarLogicSection({ title, statement, highlightedStrings, externalUrl }: HelperProps) {
    const { t } = useTranslation('entity.shared.containers');
    const { t: tv } = useTranslation('entity.profile.view');
    const isEmbeddedProfile = useIsEmbeddedProfile();

    const highlightedLineNumbers = new Set(highlightedStrings?.map((s) => findLineNumberToHighlight(statement, s)));
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const formattedLogic = baseEntity?.dataset?.viewProperties?.formattedLogic;
    const language = baseEntity?.dataset?.viewProperties?.language;

    const canShowFormatted = !!formattedLogic;

    const isDbt = baseEntity?.dataset?.platform?.urn === DBT_URN;
    const [showFormatted, setShowFormatted] = useState(false);
    const languageOptions = canShowFormatted
        ? [
              {
                  label: isDbt ? tv('viewDefinitionTab.formatSource') : tv('viewDefinitionTab.formatRaw'),
                  value: SOURCE_OPTION,
              },
              {
                  label: isDbt ? tv('viewDefinitionTab.formatCompiled') : tv('viewDefinitionTab.formatFormatted'),
                  value: FORMATTED_OPTION,
              },
          ]
        : undefined;
    const selectedLanguage = showFormatted ? FORMATTED_OPTION : SOURCE_OPTION;
    const code = showFormatted ? formattedLogic || statement : statement;
    const codeLanguage = language?.toLowerCase() ?? DEFAULT_LANGUAGE;
    const onLanguageChange = (value: string) => setShowFormatted(value === FORMATTED_OPTION);
    const openFullProfile = () => window.open(`${externalUrl}/View Definition`, '_blank', 'noopener,noreferrer');

    return (
        <SidebarSection
            title={title}
            content={
                <>
                    <CodeBlock
                        code={code}
                        language={codeLanguage}
                        languageLabel={false}
                        languageOptions={languageOptions}
                        selectedLanguage={selectedLanguage}
                        onLanguageChange={onLanguageChange}
                        showLineNumbers
                        hideLineNumbers
                        wrap
                        highlightedLines={highlightedLineNumbers}
                        maxHeight={320}
                        overflow="auto"
                        showCopy
                        showHeader
                    />
                    {isEmbeddedProfile && (
                        <Button variant="text" onClick={openFullProfile}>
                            {t('sidebar.logic.seeFullButton')}
                        </Button>
                    )}
                </>
            }
        />
    );
}

/** Find the line number of a target substring in a given string */
function findLineNumberToHighlight(inputString: string, targetSubstring: string) {
    if (!targetSubstring) {
        return -1;
    }

    const lines = inputString.split('\n');

    for (let lineNumber = 0; lineNumber < lines.length; lineNumber++) {
        if (lines[lineNumber].includes(targetSubstring)) {
            // Adding 1 because line numbers are 1-based, not 0-based
            return lineNumber + 1;
        }
    }

    // Return -1 if the target substring is not found in any line
    return -1;
}
