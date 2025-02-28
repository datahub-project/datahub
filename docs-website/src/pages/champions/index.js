import React from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Link from "@docusaurus/Link";
import ChampionQualityCardsSection from "./_components/ChampionQualityCardsSection"

import ChampionSection from "./_components/ChampionSection";

const championSections = [
  {
    people: [
      {
        name: "Patrick Braz",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/patrick-franco-braz.jpeg",
        position: "Data Engineering Specialist, Grupo Boticário",
        bio: (
            <>
              <p>
              Submitted 16 pull requests and 3 issues and regularly provides guidance to Community Members in Slack channels.
              </p>
            </>
        ),
        social: {
            linkedin: "https://www.linkedin.com/in/patrick-franco-braz/",
            github: "https://github.com/PatrickfBraz",
        },
        location: "Rio de Janeiro, Brazil"
      },
      {
        name: "Mike Burke",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/datahub-champions/initials/mb.jpg",
        position: "",
        bio: (
          <>
            <p>
            Regularly provides support to Community Members and amplifies DataHub across his network.
           </p>
          </>
        ),
        social: {
          },
      },
      {
        name: "Siladitya Chakraborty",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/datahub-champions/siladitya_chakraborty.jpeg",
        position: "Data Engineer, Adevinta",
        bio: (
          <>
            <p>
              Submitted 16 pull requests including improvements on graphQL and search API.
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/aditya-0bab9a84/",
          github: "https://github.com/siladitya2",
        },
        location: "Barcelona, Spain"
      },
      {
        name: "Tim Drahn",
        position: "Optum",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/datahub-champions/initials/td.jpg",
        position: "Solution Architect, Optum Technologies",
        bio: (
          <>
            <p>
            Submitted 2 pull requests and 1 issue while reliably providing direction to Community Members across all support channels in Slack.
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/tim-drahn-a873532b/",
          github: "https://github.com/tkdrahn",
          },
        location: "MA, USA"
      },
      {
        name: "Steve Pham",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/cuong-pham.jpeg",
        bio: (
          <>
            <p>
            Submitted 4 pull requests and reliably provided direction to Community Members across all support channels in Slack.
            </p>
          </>
        ),
        social: {
          },
      },
      {
        name: "David Schmidt",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/datahub-champions/david_schmidt.jpeg",
        position: "Data Engineer, inovex GmbH",
        bio: (
          <>
            <p>
            Regularly provides support to Community Members in Slack, submitted 4 pull requests and 6 issies.
           </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/david-schmidt-de/",
          github: "https://github.com/DSchmidtDev",
          },
      },
      {
        name: "Sudhakara ST",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/datahub-champions/initials/ss.jpg",
        position: "Engineer, Zynga",
        bio: (
          <>
            <p>
            Reliably provides direction to Community Members across all support channels in Slack and shared Zynga's experience adopting and implementing DataHub during the September 2023 Town Hall.
           </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/sudhakara-st/",
          },
        location: "Bengaluru, India"
      },
      {
        name: "Tim Bossenmaier",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/champ-img/imgs/datahub-champions/tim_bossenmaier.jpeg",
        position: "Data & Software Engineer, Cloudflight",
        bio: (
          <>
            <p>
            Reliably provides direction to community members and submitted 9 pull request, including improvements to Athena ingestion (support for nested schemas) and the REST emitter. 
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/tim-bossenmaier/",
          github: "https://github.com/bossenti",
          },
        location: "Innsbruck, Austria"
      },
      {
        name: "Raj Tekal",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/datahub-champions/initials/rt.jpg",
        position: "Lead Software Engineer, Optum Technologies",
        bio: (
          <>
            <p>
            Submitted 4 pull requests.
            </p>
          </>
        ),
        social: {
             },
        location: "PA, USA"
      },
    ],
  },
];

const HeroImage = (props) => {
  return (
    <>
      <img style={{ marginBottom: "2rem", height: "14rem" }} src={`/img/champions/champions-logo-light.png`} alt="DataHub Champions" {...props} />
    </>
  );
};

function Champion() {
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
              <div className="hero__title">DataHub Champions</div>
              <p className="hero__subtitle">
                Recognizing community members who have made exceptional contributions to further the collective success of DataHub.              
              </p>
              <ChampionQualityCardsSection />
            </div>
            <h1>Meet Our Champions</h1>
            <div style={{ textAlign: "right" }}>
              <Link className="button button--secondary button--md" to="/guild">
                See Data Practitioner Guild (2022) →
              </Link>
            </div>
          </div>
          
          {championSections.map((section, idx) => (
            <ChampionSection key={idx} {...section} />
          ))}
        </div>
      </header>
    </Layout>
  );
}

export default Champion;
