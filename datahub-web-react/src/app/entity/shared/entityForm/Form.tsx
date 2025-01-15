import React from 'react';
import styled from 'styled-components';
import { useEntityData } from '../EntityContext';
import { FormPrompt } from '../../../../types.generated';
import Prompt, { PromptWrapper } from './prompts/Prompt';
import { ANTD_GRAY_V2 } from '../constants';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { PromptSubTitle } from './prompts/StructuredPropertyPrompt/StructuredPropertyPrompt';
import SchemaFieldPrompts from './schemaFieldPrompts/SchemaFieldPrompts';
import useGetPromptInfo from '../containers/profile/sidebar/FormInfo/useGetPromptInfo';
import VerificationPrompt from './prompts/VerificationPrompt';
import useShouldShowVerificationPrompt from './useShouldShowVerificationPrompt';
import { getFormAssociation } from '../containers/profile/sidebar/FormInfo/utils';
import FormRequestedBy from './FormSelectionModal/FormRequestedBy';
import useHasComponentRendered from '../../../shared/useHasComponentRendered';
import Loading from '../../../shared/Loading';
import { DeferredRenderComponent } from '../../../shared/DeferredRenderComponent';
import { Editor } from '../tabs/Documentation/components/editor/Editor';

const TabWrapper = styled.div`
    background-color: ${ANTD_GRAY_V2[1]};
    overflow: auto;
    padding: 24px;
    flex: 1;
    max-height: 100%;
`;

const IntroTitle = styled.div`
    font-size: 20px;
    font-weight: 600;
`;

const HeaderWrapper = styled(PromptWrapper)``;

const SubTitle = styled(PromptSubTitle)`
    margin-top: 16px;
`;

const RequestedByWrapper = styled(PromptSubTitle)`
    color: ${ANTD_GRAY_V2[8]};
`;

interface Props {
    formUrn: string;
}

function Form({ formUrn }: Props) {
    const entityRegistry = useEntityRegistry();
    const { entityType, entityData } = useEntityData();
    const { entityPrompts, fieldPrompts } = useGetPromptInfo(formUrn);
    const shouldShowVerificationPrompt = useShouldShowVerificationPrompt(formUrn);
    const { hasRendered } = useHasComponentRendered();

    if (!hasRendered) return <Loading />;

    const formAssociation = getFormAssociation(formUrn, entityData);
    const title = formAssociation?.form?.info?.name;
    const associatedUrn = formAssociation?.associatedUrn;
    const description = formAssociation?.form?.info?.description;
    const owners = formAssociation?.form?.ownership?.owners;

    return (
        <TabWrapper>
            <HeaderWrapper>
                <IntroTitle>
                    {title ? <>{title}</> : <>{entityRegistry.getEntityName(entityType)} Requirements</>}
                </IntroTitle>
                {owners && owners.length > 0 && (
                    <RequestedByWrapper>
                        <FormRequestedBy owners={owners} />
                    </RequestedByWrapper>
                )}
                {description ? (
                    <SubTitle>
                        <Editor content={description} readOnly editorStyle="padding: 0;" />
                    </SubTitle>
                ) : (
                    <SubTitle>
                        Please fill out the following information for this {entityRegistry.getEntityName(entityType)} so
                        that we can keep track of the status of the asset
                    </SubTitle>
                )}
            </HeaderWrapper>
            {entityPrompts?.map((prompt, index) => (
                <Prompt
                    key={`${prompt.id}-${entityData?.urn}`}
                    promptNumber={index + 1}
                    prompt={prompt as FormPrompt}
                    associatedUrn={associatedUrn}
                />
            ))}
            {fieldPrompts.length > 0 && <SchemaFieldPrompts prompts={fieldPrompts} associatedUrn={associatedUrn} />}
            {shouldShowVerificationPrompt && <VerificationPrompt formUrn={formUrn} associatedUrn={associatedUrn} />}
        </TabWrapper>
    );
}

export default function FormContainer({ formUrn }: Props) {
    return <DeferredRenderComponent wrappedComponent={<Form formUrn={formUrn} />} />;
}
