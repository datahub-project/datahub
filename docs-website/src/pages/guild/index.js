import React from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import { useColorMode } from "@docusaurus/theme-common";

import GuildSection from "./_components/GuildSection";

const guildSections = [
  {
    name: "Top Code Contributor",
    description: "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nulla venenatis libero enim, eu porta tortor faucibus eu.",
    badge: "/img/guild/badge-top-code-contributor.svg",
    people: [
      {
        name: "Harshal Sheth",
        image: "https://avatars.githubusercontent.com/u/12566801?v=4",
        bio: "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Aenean sit amet magna aliquam, scelerisque libero eu, vestibulum magna. Pellentesque ut molestie sapien. Donec blandit placerat elementum. Duis eu elementum ligula. Nullam tincidunt accumsan risus, malesuada vestibulum non.",
        social: {
          linkedin: "https://www.linkedin.com/",
          twitter: "https://twitter.com/",
          mastodon: "https://mastodon.social/",
          github: "https://github.com",
          web: "https://www.google.com",
        },
      },
      //Generate placeholder data
      ...Array(12)
        .fill()
        .map(() => ({
          name: "FirstName LastName",
          image: "",
          bio: "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Aenean sit amet magna aliquam, scelerisque libero eu, vestibulum magna. Pellentesque ut molestie sapien. Donec blandit placerat elementum. Duis eu elementum ligula. Nullam tincidunt accumsan risus, malesuada vestibulum non.",
          social: {
            linkedin: "",
            twitter: "",
            mastodon: "",
            github: "",
            web: "",
          },
        })),
    ],
  },
  {
    name: "Community Supporter",
    description: "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nulla venenatis libero enim, eu porta tortor faucibus eu.",
    badge: "/img/guild/badge-community-supporter.svg",
    people: [
      //Generate placeholder data
      ...Array(13)
        .fill()
        .map(() => ({
          name: "FirstName LastName",
          image: "",
          bio: "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Aenean sit amet magna aliquam, scelerisque libero eu, vestibulum magna. Pellentesque ut molestie sapien. Donec blandit placerat elementum. Duis eu elementum ligula. Nullam tincidunt accumsan risus, malesuada vestibulum non.",
          social: {
            linkedin: "",
            twitter: "",
            mastodon: "",
            github: "",
            web: "",
          },
        })),
    ],
  },
  {
    name: "DataHub Champion",
    description: "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nulla venenatis libero enim, eu porta tortor faucibus eu.",
    badge: "/img/guild/badge-datahub-champion.svg",
    people: [
      //Generate placeholder data
      ...Array(13)
        .fill()
        .map(() => ({
          name: "FirstName LastName",
          image: "",
          bio: "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Aenean sit amet magna aliquam, scelerisque libero eu, vestibulum magna. Pellentesque ut molestie sapien. Donec blandit placerat elementum. Duis eu elementum ligula. Nullam tincidunt accumsan risus, malesuada vestibulum non.",
          social: {
            linkedin: "",
            twitter: "",
            mastodon: "",
            github: "",
            web: "",
          },
        })),
    ],
  },
];

const HeroImage = (props) => {
  const { colorMode } = useColorMode();
  return (
    <>
      <img style={{ marginBottom: "2rem" }} src={`/img/guild/guild-logo-${colorMode}.svg`} alt="DataHub Data Practitioners Guild" {...props} />
    </>
  );
};

function Guild() {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;

  return (
    <Layout
      title={siteConfig.tagline}
      description="DataHub is a data discovery application built on an extensible metadata platform that helps you tame the complexity of diverse data ecosystems."
    >
      <header className={"hero"}>
        <div className="container">
          <div className="hero__content">
            <div>
              <HeroImage />
              <p className="hero__subtitle">
                Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nulla venenatis libero enim, eu porta tortor faucibus eu.
              </p>
            </div>
          </div>
          {guildSections.map((section, idx) => (
            <GuildSection key={idx} {...section} />
          ))}
        </div>
      </header>
    </Layout>
  );
}

export default Guild;
