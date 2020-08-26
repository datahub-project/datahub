interface IDescriptorKind {
  // Differentiates descriptor types
  kind: 'method' | 'accessor' | 'field' | 'class' | 'hook';
}

export interface IParameterElementDescriptor extends IDescriptorKind {
  kind: 'method' | 'accessor' | 'field' | 'hook';
  // Method or property name
  key: string | symbol;
  placement: 'prototype' | 'static' | 'own';
  // Function used to set the initial value of the field.
  // initialize function is present only if the field is initialized into the class body: field = 10.
  // A declaration like field; has initialize as undefined. Ascertain presence before invocation
  initialize?: Function;
  // method function present on a method decorator
  method?: Function;
  descriptor: PropertyDescriptor;
}

export interface IReturnElementDescriptor extends IParameterElementDescriptor {
  // Array of decorator descriptors, not property descriptors
  extras: Array<DecoratorDescriptor>;
  // Hook, similar to field initializers except they do not result in defining a field
  // These can be used, e.g., to make a decorator which defines fields through [[Set]] rather than [[DefineOwnProperty]], or to store the field in some other place.
  start?(): unknown;
  replace?(): unknown;
  finish?(): unknown;
}

export interface IParameterClassDescriptor extends IDescriptorKind {
  kind: 'class';
  // List of decorator descriptors not to be confused with property descriptors
  elements: Array<DecoratorDescriptor>;
}

export type DecoratorDescriptor = IParameterClassDescriptor | IReturnElementDescriptor | IParameterElementDescriptor;
