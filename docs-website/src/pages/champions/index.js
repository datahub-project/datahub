import React from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import { useColorMode } from "@docusaurus/theme-common";
import Link from "@docusaurus/Link";
import ChampionQualityCardsSection from "./_components/ChampionQualityCardsSection"

import ChampionSection from "./_components/ChampionSection";

const championSections = [
  {
    people: [
      {
        name: "Piotr Skrydalewicz",
        position: "Data Engineer",
        bio: (
          <>
            <p>
              Contributed 5 commits in 2022 to the main DataHub Project & Helm repos, including Stateful Ingestion support for Presto-on-Hive
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/skrydal",
        },
        location: "Lodz, Poland"
      },
      {
        name: "Siladitya Chakraborty",
        position: "Data Engineer, Adevinta",
        bio: (
          <>
            <p>
              Driving DataHub adoption at Adevinta            
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/aditya-0bab9a84/",
          github: "https://github.com/siladitya2",
        },
        locataion: "Barcelona, Spain"
      },
      {
        name: "Sergio Gómez Villamor",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/sergio-gomez-villamor.jpeg",
        position: "Tech Lead, Adevinta",
        bio: (
          <>
            <p>
            Submitted 26 pull requests and 4 issues in total and featured in Humans of DataHub
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/sgomezvillamor/",
          github: "https://github.com/sgomezvillamor/",
          },
        location: "Barcelona,Spain"
        },
      {
        name: "Amanda Ng",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/amanda-ng.png",
        position: "Lead Software Engineer, Grab",
        bio: (
          <>
            <p>
            Submitted 9 pull requests and shared Grab's expereince adopting and implementing DataHub during October 2022 Town Hall
            </p>
          </>
        ),
        social: {
          linkedin: "https://sg.linkedin.com/in/amandang19",
          github: "https://github.com/ngamanda",
          },
        location: "Singapore"
        },
      {
        name: "Harvey Li",
        position: "Lead Data Engineer, Grab",
        bio: (
          <>
            <p>
            Shared Grab's expereince adopting and implementing DataHub during October 2022 Town Hall and featured in Humans of datahub
            </p>
          </>
        ),        
        social: {
          linkedin: "https://www.linkedin.com/in/li-haihui",
          github: "https://github.com/HarveyLeo",
          },
        },
      {
        name: "Fredrik Sannholm",
        position: "",
        bio: (
          <>
            <p>
              Driving DataHub adoption at Wolt and featured in Humans of DataHub            
            </p>
          </>
        ),        
        social: {
          },
        },
      {
        name: "Tim Bossenmaier",
        position: "Data & Software Engineer, Bytefabrik.AI",
        bio: (
          <>
            <p>
            Reliably provides direction to Community Members across all support channels in Slack
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/tim-bossenmaier/",
          github: "https://github.com/bossenti",
          },
        location: "Sigmaringen, Germany"
        },
      {
        name: "Nikola Kasev",
        position: "Data Engineer, KPN",
        bio: (
          <>
            <p>
            Reliably provides direction to Community Members across all support channels in Slack
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/nikolakasev",
          github: "https://github.com/nikolakasev",
          },
        location: "Haarlem, Noord-holland"
        },
      {
        name: "Nidhin Nandhakumar",
        bio: (
          <>
            <p>
            </p>
          </>
        ),
        social: {
          },
        },
      {
        name: "Patrick Braz",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/patrick-franco-braz.jpeg",
        position: "Data Engineering Specialist, Grupo Boticário",
        bio: (
          <>
            <p>
            Submitted 16 pull requests and 3 issues and regularly provides guidance to Community Members in Slack channels
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
        name: "Steve Pham",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/cuong-pham.jpeg",
        bio: (
          <>
            <p>
            Submitted 4 pull requests and reliably provides direction to Community Members across all support channels in Slack
            </p>
          </>
        ),
        social: {
          },
      },
      {
        name: "Wu Teng",
        bio: (
          <>
            <p>
              Reliably provides direction to Community Members across all support channels in Slack
            </p>
          </>
        ),
        social: {
          },
      },
      {
        name: "Felipe Gusmao",
        bio: (
          <>
            <p>
            Shared Zynga's expereince adopting and implementing DataHub during September 2023 Town Hall
            </p>
          </>
        ),
        social: {
          },
       },
       {
        name: "Sudhakara",
        position: "Engineer, Zynga",
        bio: (
          <>
            <p>
            Reliably provides direction to Community Members across all support channels in Slack and 
            shared Zynga's expereince adopting and implementing DataHub during September 2023 Town Hall
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/sudhakara-st/",
          },
        location: "Bengaluru, India"
      },
      {
        name: "Bobbie-Jean Nowak",
        position: "Technical Product Manager,	Optum ",
        bio: (
          <>
            <p>
            Submitted 16 pull requests and 3 issues and regularly provides guidance to Community Members in the #troubleshoot and #ingestion Slack channels"
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/bobbie-jean-nowak-a0076b77/",
          },
        location: "Minnesota, USA"
      },
      {
        name: "Dima Korendovych",
        bio: (
          <>
            <p>
            </p>
          </>
        ),
        social: {
          },
      },
      {
        name: "Tim Drahn",
        position: "Solution Architect, Optum Technologies",
        bio: (
          <>
            <p>
            Submitted 2 pull requests and 1 issue while reliably provides direction to Community Members across all support channels in Slack
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
        name: "Kate Koy",
        bio: (
          <>
            <p>
            </p>
          </>
        ),
        social: {
          },
      },
      {
        name: "Anjali Arora",
        bio: (
          <>
            <p>
            </p>
          </>
        ),
        social: {
          },
      },
      {
        name: "Raj Tekal",
        position: "Lead Software Engineer, Optum Technologies",
        bio: (
          <>
            <p>
            Submitted 4 pull requests 
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/patrick-franco-braz/",
          github: "https://github.com/PatrickfBraz",
          },
        location: "PA, USA"
      },
    ],
  },
];

const HeroImage = (props) => {
  const { colorMode } = useColorMode();
  return (
    <>
      <img style={{ marginBottom: "2rem", height: "14rem" }} src={`/img/champions/champions-logo-${colorMode}.png`} alt="DataHub Champions" {...props} />
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
              <h1>DataHub Champions</h1>
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
