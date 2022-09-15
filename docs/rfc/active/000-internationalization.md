- Start Date: (2022-09-14)
- RFC PR: (after opening the RFC PR, update this with a link to it and update the file name)
- Discussion Issue: (None)
- Implementation PR(s): (leave this empty)

# Internalization (i18n)

## Summary
> Implements the internalization of Datahub.
> Being able to change and add language without modifying the source.

## Basic example
```javascript
// Instead of this
<Button type="submit">
    Next
</Button>
// We write this
<Button type="secondary">
    {t('common.next')}
</Button>
// Or
<Button type="secondary">
    <Trans i18nKey="common.next"/>
</Button>
// And the display is the same in english, but translated in choosen language.
```

## Motivation
Why are we doing this?
> I have implemented for my company a french version of Datahub (or 90% of it). 
> Now I have difficulties pulling new versions so I think it would be beneficial for both of 
> us to share my modifications.
>
What use cases does it suppor
> Anyone who would want to have their own language for the Datahub will be able to do so.
>
What is the expected outcome?
> If I finish this as I expect it, Datahub will be able to change dynamically his language, no reload needed.
> And anybody will be able to add a language just by adding a *translation.json* file in the correct directory.
>

## Requirements
> - [ ] react-i18next
> - [x] Uniformisation of Datahub types
> - [ ] Uniformisation of Datahub subtypes
> - [ ] Uniformisation of frontend displaying of exceptions
> - [ ] Factorisation of Dates
> - [ ] Factorisation of numbers (count, filter, table total, etc)

### Extensibility
> Anybody can add his own language by adding the a *translation.json* file in the corresponding directory.

## Non-Requirements
> I won't talk about which language should be implemented or not. English is native. 
> I will take care of French. If anyone want his own language, let him be. 

## Detailed design

### Terminology
> i18n : Internationalisation (begin with an "i", end with a "n" and have 18 letters between)
>

### Overview
> I am using [react-i18next](https://react.i18next.com/) to do so. Basically we wrapp the App in a I18nestProvider
> which allow us to call a hook or a render prop. We give them a key string which point on a *translation.json*
> containing the text we want (according to the choosen language). Example from the react-i18next doc:
```javascript
// Example hook
import React from 'react';
import { useTranslation } from 'react-i18next';

export function MyComponent() {
    const { t, i18n } = useTranslation();
    // or const [t, i18n] = useTranslation();

    return <p>{t('type.translatedText')}</p>
}
```
```javascript
// Example component
import React from 'react';
import { Trans } from 'react-i18next';

export function MyComponent() {
    return (
        <Trans i18nKey="type.translatedText"/>
    )
}
```
> You can also pass dynamic text, or pass some html (but you have to use the Trans component for that).
> 

> In any case, if the translation doesn't exist in requested language, then it give the default (ie. English) translation.
> If it doesn't exist in the default language, then it return the given translation key.
> So the worste case scenario is an ugly translation, but no blocking bugs.
>

> By default, it can be configured to use the browser language
> (with a fallback to the default language if browser language is not translated).
>

> In practice, I think the lightest way for the Datahub team would be to just implement the english language. 
> Leaving any other language for others devs. Since we should be able to dynamically check how many language there is and which one are they,
> we could add a "Change Language" field in the Header Menu opening a modal where you can select your desired language.
> And obviously, we hide it if there is only one language selectable.


### Corner-case

#### Front end test
> There are tests in the frontend looking for the presence of text. I have deactivated them.
> I am not sure how react-i18next will render in a test environment.
> 

#### Back end text
> Some text come from the back end.
> - The back-end exceptions : we can handle then by adding a translate key to exceptions, and the front will
    > have to apply i18next on the the translate key to display it.
> - The whole Analytics page : It is one of the majors problem... All text of the Analytics page is built,
    > almost word by word, by the gms... I am not sure what is the best way here... Maybe moving theses constructs
    > from the back to the front...

#### Text size
> Depending on the language, some text length may vary. This could be a problem on buttons or narrow panel for exemple
> 

#### Other libraries
> When using other libraries, they may not have an i18n interface. For example the yaml editor in *YamlEditor.tsx*.
> 

## How we teach this
> Since I am thinking about adding only the architecture for the i18n, it will be usefull only for developers.
> But any developer who want to add a new language can follow some very brief instructions, a single page for internationalization 
> should be enough. The only thing to do is to copy an English *translation.json* and write it in desired language.
>

## Drawbacks
> The unique but wearisome drawbacks is that the developers won't be able to write direct text in their 
> component / exceptions. They'll have to report, and distribute correctly, their text in a *translation.json* file.
> 

## Alternatives
What other designs have been considered?
> None
>

What is the impact of not doing this?
> None
>

## Rollout / Adoption Strategy
> Once it is done, there won't be any modifications to do for people whom use Datahub as it is.
> For anyone who have made a fork, it could have an impact if they added type, subtype or any generic word,
> the generated translation could be ugly. like *'entity.NEW_TYPE.plural'* instead of a desired *'New Type'*.
> But I think we can add a default display if the generic key have no translation to avoid that.
> 

## Future Work
> It could be used in any project that plan to make the Datahub more accessible for anyone who's not fluent in english.

## Unresolved questions
> As you may have guessed throught what I wrote, I am still not certain about :
> - Default text if translation key doesn't exist in default language.
> - Being able to calculate automatically how many language are implemented.