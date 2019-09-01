// Flacky test: what if image URL fails too?
const imageUrlFallback = '/assets/default_avatar.png';

export default [
  {
    name: 'AvatarName1',
    imageUrl: 'http://lorempixel.com/400/400',
    imageUrlFallback,
    email: 'AvatarName1@example.com',
    userName: 'AvatarUserName1'
  },
  {
    name: 'AvatarName2',
    imageUrl: 'http://lorempixel.com/400/400',
    imageUrlFallback,
    email: 'AvatarName2@example.com',
    userName: 'AvatarUserName2'
  },
  {
    name: 'AvatarName3',
    imageUrl: 'http://lorempixel.com/400/400',
    imageUrlFallback,
    email: 'AvatarName3@example.com',
    userName: 'AvatarUserName3'
  },
  {
    name: 'AvatarName4',
    imageUrl: 'http://lorempixel.com/400/400',
    imageUrlFallback,
    email: 'AvatarName4@example.com',
    userName: 'AvatarUserName4'
  }
];
