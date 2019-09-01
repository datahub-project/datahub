/**
 * Extending nearley types that are missing in their typings file.
 */
import { Grammar, Rule, Parser as NearlyParserBaseType } from 'nearley';

export class Parser extends NearlyParserBaseType {
  table?: Array<Column>;
}

export class State {
  data: Array<string> | string;
  dot: number;
  isComplete: boolean;
  left?: State;
  reference: number;
  right?: State;
  rule: Rule;
  wantedBy: Array<State>;
}

export class Column {
  completed: {};
  grammar: Grammar;
  index: number;
  scannable: Array<State>;
  states: Array<State>;
  wants: Record<string, State>;
}
