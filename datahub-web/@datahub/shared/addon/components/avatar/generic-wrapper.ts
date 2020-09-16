import Component from '@glimmer/component';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';

interface IAvatarGenericWrapperArgs {
  // Optional parameters to influence what renders in the generic wrapper
  options: {
    // Component to accept the yielded params of the avatar main container, for example avatar-name
    component: string;
    // Whether or not we pass the whole of the yielded avatars to a single component that accepts
    // a list or if we want to use the above stated component for each avatar in the yielded list
    // from avatar main container
    avatarsAsList?: boolean;
    // Whether or not we should build the avatars in the avatar main container. Most often this
    // will likely be false
    shouldBuildAvatar?: boolean;
  };
  // The entity we are passing to avatar main container to be transformed. Called "value" as this
  // is often the generic param used to pass values into generic components in our application
  value: PersonEntity | Array<PersonEntity>;
}

/**
 * The generic wrapper component can be used to wrap both the avatar main container and a avatar
 * presentational component together, useful for when the component is being loaded to a config
 * and the template doesn't have room to allow for the full blown container usage that requires
 * yielding to block components and passing different params around, etc.
 * Note that if the generic wrapper is used, the value is expected to be a person entity
 */
export default class AvatarGenericWrapper extends Component<IAvatarGenericWrapperArgs> {}
