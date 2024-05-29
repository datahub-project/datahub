/* eslint-disable */
// TODO: Cleanup linting errors

import React, { useState, useEffect, useRef } from 'react';

import { Input } from 'antd';

import {
	Step,
	StepHeader,
	StepField,
	StepButtons,
} from './components';

import { SecondaryButton } from '../sharedComponents';

import { getSteps } from './utils';

import { TermSelector } from './fields/TermSelector';
import { ConnectionSelector } from './fields/ConnectionSelector';
import { CategorySelector } from './fields/CategorySelector';
import { TraversalSelector } from './fields/TraversalSelector';
import { CustomActionSelector } from './fields/CustomActionSelector';
import { DataAssetSelector } from './fields/DataAssetSelector';
import { ConditionSelector } from './fields/ConditionSelector';

export const Configure = ({ automation, initData, formData, setFormData }: any) => {
	const steps = automation.steps || getSteps(automation);
	const prevProps = useRef(formData);

	// Various field states
	const [actionSelection, setActionSelection] = useState<string[]>([]);
	const [predicateSelection, setPredicateSelection] = useState<string[]>([]);
	const [assetTypesSelected, setAssetTypesSelected] = useState<string[]>([]);
	const [termsSelected, setTermsSelected] = useState<string[]>([]);
	const [connectionSelected, setConnectionSelected] = useState<string | undefined>();
	const [categorySelected, setCategorySelected] = useState<string | undefined>();
	const [details, setDetails] = useState<any>({});

	// Initialize the form data
	useEffect(() => {
		if (initData) {
			// Handle recipe info
			if (initData.definition?.config?.recipe) {
				const recipe = JSON.parse(initData.definition.config.recipe);
				const terms = recipe?.action?.config?.term_propagation?.target_terms;
				if (termsSelected.length === 0 && terms?.length > 0)
					setTermsSelected(recipe.action.config.term_propagation.target_terms);
			}

			// Handle Category
			if (!categorySelected && initData.category) setCategorySelected(initData.category);

			// Handle Details
			if ((!details.name || !details.description) && (initData.name || initData.description)) {
				setDetails({
					name: initData.name,
					description: initData.description,
				});
			}
		}
	}, [initData, setCategorySelected, setDetails]);

	// Form Data to be submitted
	const data = {
		terms: termsSelected,
		connection: connectionSelected,
		predicates: predicateSelection,
		actions: actionSelection,
		category: categorySelected,
		source: assetTypesSelected,
		...details
	};

	// Send the form data back to the parent component
	// Only sends the data if the form data has changed
	useEffect(() => {
		if (JSON.stringify(prevProps.current) !== JSON.stringify(data)) {
			setFormData(data);
		}
		prevProps.current = data;
	}, [data]);

	return (
		<div>
			{steps.map((step: any, index: number) => (
				<Step key={index}>
					{/* Header */}
					<StepHeader>
						<h2>{step.title}</h2>
						<p>{step.description}</p>
					</StepHeader>

					{/* Fields */}
					{step.fields.map((field: any, index: number) => {
						return (
							<StepField key={index}>
								{/* Field Label */}
								{field.label && (
									<label>
										{field.label}
										{field.isRequired && <sup>*</sup>}
									</label>
								)}

								{/* Term Selector */}
								{field.type === 'termSelector' && (
									<TermSelector
										termsSelected={termsSelected}
										setTermsSelected={setTermsSelected}
										isRequired={field.isRequired}
									/>
								)}

								{/* Connection Selector */}
								{field.type === 'connectionSelector' && (
									<ConnectionSelector
										connectionTypes={step.connectionTypes}
										connectionSelected={connectionSelected}
										setConnectionSelected={setConnectionSelected}
										isRequired={field.isRequired}
									/>
								)}

								{/* Traversal Selector */}
								{field.type === 'traversalSelector' && (
									<TraversalSelector />
								)}

								{/* Custom Actions */}
								{field.type === 'customActionSelector' && (
									<CustomActionSelector />
								)}

								{/* Data Asset Selector */}
								{field.type === 'dataAssetSelector' && (
									<DataAssetSelector
										dataAssetSelected={assetTypesSelected}
										setDataAssetSelected={setAssetTypesSelected}
									/>
								)}

								{/* Condition Selector */}
								{field.type === 'conditionSelector' && (
									<ConditionSelector />
								)}

								{/* Category Selector */}
								{field.type === 'categorySelector' && (
									<CategorySelector
										categorySelected={categorySelected}
										setCategorySelected={setCategorySelected}
										isRequired={field.isRequired}
									/>
								)}

								{/* Text Input */}
								{field.type === 'text' && (
									<Input
										type="text"
										value={field.label && data[field.label.toLowerCase()]}
										onChange={(e) => setDetails({ ...details, [field.label.toLowerCase()]: e.target.value })}
										required={field.isRequired}
									/>
								)}

								{/* Text Area */}
								{field.type === 'longtext' && (
									<Input.TextArea
										value={field.label && data[field.label.toLowerCase()]}
										onChange={(e) => setDetails({ ...details, [field.label.toLowerCase()]: e.target.value })}
										required={field.isRequired}
									/>
								)}
							</StepField>
						)
					})}

					{/* Test / Preview Buttons */}
					{/* {step.canTest || step.canPreview && (
						<StepButtons>
							{step.canTest && <SecondaryButton disabled>{step.testTitle}</SecondaryButton>}
							{step.canPreview && <SecondaryButton disabled>{step.previewTitle}</SecondaryButton>}
						</StepButtons>
					)} */}
				</Step>
			))}
		</div>
	);
}