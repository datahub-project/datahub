import { computed, set } from '@ember/object';

// This interface should be augmented by different entities
// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface IAvailableAspects {}

/**
 * The class that is going to support aspects should follow this interface
 */
interface IAspectContainer {
  aspectsLoaded?: Array<string>;
  aspectBag?: IAvailableAspects;
  aspectMapKeyToName?: Record<string, string>; // entityTopUsage => com.linkedin.common.EntityTopUsage
  entity?: {};
}

/**
 * Aspect decorator, to decorate properties with the aspect name.
 * It will link to the entity propery name (same as the variable name for now), but
 * if the aspect is defined in the aspect bag, it will take preference.
 *
 * @param aspectName aspect name, for example, com.linkedin.common.Likes
 */
export const aspect = function(aspectName: string) {
  return function(...args: [IAspectContainer, string]): void {
    const [target, propertyKey] = args;
    const { aspectMapKeyToName } = target;
    set(target, 'aspectMapKeyToName', {
      ...aspectMapKeyToName,
      [propertyKey]: aspectName
    });
    return computed(`entity.${propertyKey}`, `aspectBag`, function(this: IAspectContainer) {
      const { entity, aspectBag } = this;
      const value = entity && (entity as Record<string, unknown>)[propertyKey];
      const aspectValue = aspectBag && aspectBag[aspectName as keyof IAvailableAspects];
      return aspectValue || value;
    })(...args);
  };
};

/**
 * Will transform the aspect name (ie com.xxxx) to the key (ie likes)
 * @param target
 * @param key
 */
const getAspectName = (target: IAspectContainer, key: string): string => {
  const aspectName = target.aspectMapKeyToName && target.aspectMapKeyToName[key];
  return aspectName || key; // if we dont have the key we assume that you actually passed a name
};

/**
 * See addAspectLoaded for better contextual understanding.
 *
 * This will check if the aspect is present or not in the aspect bag
 *
 * @param target the specific entity which the aspects are loaded
 * @param aspect the name of the aspect that we want to check (com.linkedin.common.Likes)
 */
export const hasAspect = (target: IAspectContainer, aspectNameOrKey: string): boolean => {
  const aspectsLoaded = target.aspectsLoaded || [];
  const aspectName = getAspectName(target, aspectNameOrKey);
  return aspectsLoaded.indexOf(aspectName) >= 0;
};

/**
 * Will add an aspect to the aspectsLoaded list. For example, search will ask the api for specific
 * aspects. In that case, search will assume that the aspects are returned and will mark those aspects as loaded.
 *
 * The intent of this array is to act as a flag. That way, we can differenciate if the aspect is not available (no data) or if the aspect
 * was simply not queried.
 *
 * @param target the specific entity which the aspects are loaded
 * @param aspectName the aspect that is loaded
 */
export const addAspectLoaded = (target: IAspectContainer, aspectNameOrKey: string): void => {
  const aspectName = getAspectName(target, aspectNameOrKey);
  if (!hasAspect(target, aspectName)) {
    set(target, 'aspectsLoaded', [...(target.aspectsLoaded || []), aspectName]);
  }
};

/**
 * Will set the aspect into the aspect bag so it is readable by the @aspect decorator
 * @param target specific entity that we want to set the aspect
 * @param aspectName name of the aspect (com.linkedin.common.Likes)
 * @param value value of the aspect
 */
export const setAspect = <T extends keyof IAvailableAspects>(
  target: IAspectContainer,
  aspectKey: T,
  value: IAvailableAspects[T]
): void => {
  const aspectName = getAspectName(target, aspectKey);

  target.aspectBag = target.aspectBag || {};

  addAspectLoaded(target, aspectName);

  set(target, 'aspectBag', {
    ...target.aspectBag,
    [aspectName]: value
  });
};
