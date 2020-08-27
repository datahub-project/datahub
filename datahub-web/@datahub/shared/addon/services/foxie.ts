import Service from '@ember/service';
import { IUserFunctionObject } from '@datahub/shared/types/foxie/user-function-object';
import { foxieTriggers } from '@datahub/shared/constants/foxie/trigger-definitions';
import {
  IFoxieScenario,
  IFoxieScenarios,
  IScenariosTracker,
  IFoxieTrigger,
  IFoxieComputeTrigger
} from '@datahub/shared/types/foxie/service';
import { inject as service } from '@ember/service';
import { tracked } from '@glimmer/tracking';
import { action } from '@ember/object';
import UserSettingsService from '@datahub/shared/services/user-settings';

/**
 * Foxie is our implementation of a "contextual based user assistance" interface in which a virtual assistant will
 * appear and provide help to the user whenever it seems that they have stumbled upon something that may require
 * additional help
 */
export default class FoxieService extends Service {
  /**
   * Injection of the user settings service so that we can read settings for foxie from
   */
  @service('user-settings')
  userSettings!: UserSettingsService;

  /**
   * List of configured trigger parameters to compare to in order to determine whether or not the UI should be
   * triggered
   */
  scenarios: IFoxieScenarios = foxieTriggers;

  /**
   * A map tracking which triggers have already been met for which scenarios. Necessary for scenarios with multiple triggers.
   *
   * @example
   * {
   *  <scenarioName>: { indexOfLastTriggeredTrigger: 0 }
   * }
   */
  scenariosTracker: IScenariosTracker = {};

  /**
   * If a foxie action has been triggered, store that here
   */
  @tracked
  currentTriggeredAction?: IFoxieScenario['actionParameters'];

  /**
   * Whether or not the service should be active. If the service is active, then met trigger parameters will activate
   * the UI to pop up the assistant alerts. If not, then the service should still be running in the background but
   * without UI results --- this is so if the user does reactivate the assistant it still maintains enough context to
   * provide the best possible suggestions based on actions up til that point.
   */
  @tracked
  isActive: boolean;

  constructor() {
    // eslint-disable-next-line prefer-rest-params
    super(...arguments);

    const initialIsActiveState = this.userSettings.getUserSetting('isVirtualAssistantActive', { default: true });
    this.isActive = Boolean(initialIsActiveState);
  }

  /**
   * Registers a user action or consequence thereof, to be compared to our triggers and see if the user has performed
   * some kind of action that should trigger an alert from the foxie service and foxie UI
   * @param userFunctionObject - the function object that should be registered with foxie
   */
  launchUFO(userFunctionObject: IUserFunctionObject): void {
    const scenarioNames = Object.keys(this.scenarios);

    if (!scenarioNames.length) {
      return;
    }

    scenarioNames.map(name => {
      const scenario = this.scenarios[name];

      // check if we are already tracking this scenario because one or more of its triggers have already been triggered
      if (this.scenariosTracker[name]) {
        const { indexOfLastTriggeredTrigger } = this.scenariosTracker[name];
        const nextTriggerIndex = indexOfLastTriggeredTrigger + 1;
        // compare ufo to this scenario's next trigger
        const doesMatch = this.doesUFOMatchTrigger(userFunctionObject, scenario.triggers[nextTriggerIndex]);

        if (doesMatch) {
          // check if this is the final trigger for this scenario
          if (nextTriggerIndex === scenario.triggers.length - 1) {
            this.currentTriggeredAction = scenario.actionParameters;
            delete this.scenariosTracker[name];
            // otherwise let's start tracking this scenario
          } else {
            this.scenariosTracker[name].indexOfLastTriggeredTrigger++;
          }
        }
      } else {
        // compare ufo to the first trigger for this scenario
        const doesMatch = this.doesUFOMatchTrigger(userFunctionObject, this.scenarios[name].triggers[0]);

        if (doesMatch) {
          // if there's only 1 trigger for this scenario, update currentTriggeredAction
          if (scenario.triggers.length === 1) {
            this.currentTriggeredAction = scenario.actionParameters;
            // otherwise, let's start tracking this scenario
          } else {
            this.scenariosTracker[name] = { indexOfLastTriggeredTrigger: 0 };
          }
        }
      }
    });
  }

  /**
   * Check whether UFO matches a given trigger, either via the trigger's computeFromObject function, or by directly
   * comparing props
   *
   * @param ufo - the user funciton object
   * @param trigger - the trigger to compare against
   */
  doesUFOMatchTrigger(ufo: IUserFunctionObject, trigger: IFoxieTrigger): boolean {
    if (typeof (trigger as IFoxieComputeTrigger).computeFromObject === 'function') {
      return (trigger as IFoxieComputeTrigger).computeFromObject(ufo);
    } else {
      return (
        ufo.functionType === (trigger as IUserFunctionObject).functionType &&
        ufo.functionTarget === (trigger as IUserFunctionObject).functionTarget &&
        ufo.functionContext === (trigger as IUserFunctionObject).functionContext
      );
    }
  }

  /**
   * Toggles whether or not the virtual assistant is active, and writes back to the user settings to maintain the
   * persisted version of this state
   */
  @action
  toggleFoxieActiveState(): void {
    this.toggleProperty('isActive');
    this.userSettings.setUserSetting('isVirtualAssistantActive', this.isActive);
  }

  @action
  onDismiss(): void {
    this.currentTriggeredAction = undefined;
  }
}

// DO NOT DELETE: this is how TypeScript knows how to look up your services.
declare module '@ember/service' {
  // This is a core ember thing
  //eslint-disable-next-line @typescript-eslint/interface-name-prefix
  interface Registry {
    foxie: FoxieService;
  }
}
