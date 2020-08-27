import { IUserFunctionObject } from '@datahub/shared/types/foxie/user-function-object';
import { IDynamicComponent } from '@datahub/shared/types/dynamic-component';

interface IFoxieComputeTrigger {
  // Instead of a direct comparison, allows us to take the object and compute whether or not the condition has been met
  // (useful for when we want to compare the object against multiple possible values for a match or something
  computeFromObject: (obj: IUserFunctionObject) => boolean;
}

/**
 * In a configuration for the service trigger, this defines what triggers the service and the possible behaviors
 * associated with that
 *
 * @example
 * This denotes we hit the browse route
 * {
 *   functionType: UserFunctionType.Navigation,
 *   functionTarget: 'browse'
 * }
 *
 * This is a trigger for if we are browsing datasets or metrics in particular:
 * {
 *   computeFromObject(obj) {
 *     return obj.functionContext === 'datasets' || object.functionContext === 'metrics';
 *   }
 * }
 *
 * Normally the service will compare a trigger (if it looks like a user function object) to determine if a condition
 * has been met for the trigger. For example, if our service had a method
 *
 * doesTriggerMatchUFO(ufo: IUserFunctionObject, trigger: IFoxieTrigger): boolean {
 *  return ufo.functionType === trigger.functionType
 *    && ufo.functionTarget === trigger.functionTarget
 *    && ufo.functionContext === trigger.functionContext
 * }
 *
 * However for a more complex case, we would be able to use compute instead:
 * doesTriggerMatchUFO(ufo: IUserFunctionObject, trigger: IFoxieTrigger): boolean {
 *   if (typeof trigger.compute === 'function') {
 *     return trigger.compute(ufo);
 *   } else {
 *     return ufo.functionType === trigger.functionType
 *       && ufo.functionTarget === trigger.functionTarget
 *       && ufo.functionContext === trigger.functionContext
 *   }
 * }
 */
export type IFoxieTrigger = IUserFunctionObject | IFoxieComputeTrigger;

/**
 * These are objects found in our configurations that define when the assistant should be triggered and what should be
 * the result.
 */
export interface IFoxieScenario {
  // The trigger or sequence of triggers that would trigger this particular
  triggers: Array<IFoxieTrigger>;

  actionParameters: {
    // string if just a message, component params if more complex action
    // IDynamicComponent references current dynamic component templates
    // { componentName: string, options: Object }
    init: string | IDynamicComponent;
  };
}

/**
 * Map of all scenarios
 */
export type IFoxieScenarios = Record<string, IFoxieScenario>;

/**
 * For scenarios with multiple triggers, we need to keep track of which triggers have already been triggered
 * Triggers must be met sequentially, so here we track the index of the last triggered trigger.
 * When a new UFO comes in, we will compare it to the trigger at indexOfLastTriggeredTrigger + 1
 */
export interface ITriggerTracker {
  indexOfLastTriggeredTrigger: number;
}

/**
 * Map of all trigger trackers
 */
export type IScenariosTracker = Record<string, ITriggerTracker>;
