import { Parser, State } from 'datahub-web/typings/modules/nearley';
import { DataModelEntity } from '@datahub/data-models/constants/entity';

/**
 * Grammar rules that we are interested
 */
export enum AutocompleteRuleNames {
  FacetName = 'FacetName',
  FacetValue = 'FacetValue',
  AndOperator = 'AndOperator',
  OrOperator = 'OrOperator',
  EntityName = 'EntityName',
  FacetSeparator = 'FacetSeparator'
}

/**
 * Map of processor. We will use this to map grammar rules with processing steps.
 *
 * Strategy family of interchangeable grammar node algorithms / processors for different rules
 * @type {Partial<Record<keyof typeof AutocompleteRuleNames, (builder?: ISuggestionBuilder, ruleState?: IState) => Promise<ISuggestionBuilder>>>}
 */

export type INodeProcessor = Partial<
  Record<
    keyof typeof AutocompleteRuleNames,
    (builder?: ISuggestionBuilder, ruleState?: IState) => Promise<ISuggestionBuilder>
  >
>;

/**
 * Same ideas as INodeProcessor but with type of facets this time.
 * Strategy family of interchangeable grammar node algorithms / processors for different facet rules
 * TODO META-6999: make facets dynamic
 */
export type INodeFacetProcessor = Record<
  string,
  (builder?: ISuggestionBuilder, facetValue?: string) => Promise<ISuggestionBuilder>
>;

/**
 * This structure will help us to comunicate state between processors.
 *
 * The structure is not intented to be mutated, redux like approach
 */
export interface ISuggestionBuilder {
  entity: DataModelEntity;
  logicalOperators: Array<ISuggestion>;
  facetNames: Array<ISuggestion>;
  datasets: Array<ISuggestion>;
  allWantedRulesMap: StateNodeMap;
  wantedRulesMap: StateNodeMap;
  parser?: Parser;
  text?: string;
  textLastWord?: string;
  textPrevious?: string;
  groups: Array<ISuggestionGroup>;
}

/**
 * Expected ember power select group structure
 */
export interface ISuggestionGroup {
  groupName: string;
  options: Array<ISuggestion>;
  disabled?: boolean;
}

/**
 * Expected suggestion estructure to render it on the UI
 */
export interface ISuggestion {
  title: string;
  text: string;
  description?: string;
  disabled?: boolean;
}

/**
 * Expanded Nearley state with extra information
 */
export interface IState extends State {
  from?: IState;
}

/**
 * A function that processes an instance of ISuggestionBuilder and returns
 * an instance of ISuggestionBuilder or a Promise object that resolves with it
 * @export
 * @interface IGrammarProcessFn
 */
export interface IGrammarProcessFn {
  (builder: ISuggestionBuilder): Promise<ISuggestionBuilder> | ISuggestionBuilder;
}

/**
 * When we select nodes from the grammar tree, we will save in this structure.
 */
export type StateNodeMap = Record<string, IState>;
