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
        name: "Siladitya Chakraborty",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/datahub-champions/siladitya_chakraborty.jpeg",
        position: "Data Engineer, Adevinta",
        bio: (
          <>
            <p>
              Submiitted 6 pull requests including improvements on graphQL and search API.
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
        name: "Sergio Gómez Villamor",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/champ-img/imgs/datahub-champions/sergio_gomez_villamor.jpg",
        position: "Tech Lead, Adevinta",
        bio: (
          <>
            <p>
            Submitted 26 pull requests and raised 4 issues, also featured in "Humans of DataHub."
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
            Submitted 9 pull requests and shared Grab's experience adopting and implementing DataHub during the October 2022 Town Hall.
            </p>
          </>
        ),
        social: {
          linkedin: "https://sg.linkedin.com/in/amandang19",
          github: "https://github.com/ngamanda",
          web: "https://ngamanda.com/",
          },
        location: "Singapore"
        },
        {
          name: "Patrick Braz",
          image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/patrick-franco-braz.jpeg",
          position: "Data Engineering Specialist, Grupo Boticário",
          bio: (
            <>
              <p>
              Submitted 16 pull requests and 3 issues and regularly provided guidance to Community Members in Slack channels.
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
              Submitted 4 pull requests and reliably provided direction to Community Members across all support channels in Slack.
              </p>
            </>
          ),
          social: {
            },
        },
        {
          name: "Piotr Skrydalewicz",
          image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/ps_white.jpg",
          position: "Data Engineer",
          bio: (
            <>
              <p>
                Contributed 5 commits in 2022 to the main DataHub Project and Helm repositories, including Stateful Ingestion support for Presto-on-Hive.
              </p>
            </>
          ),
          social: {
            linkedin: "https://www.linkedin.com/in/skrydal",
          },
          location: "Lodz, Poland"
        },
      {
        name: "Harvey Li",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/hl_black.jpg",
        position: "Lead Data Engineer, Grab",
        bio: (
          <>
            <p>
            Shared Grab's experience adopting and implementing DataHub during the October 2022 Town Hall and featured in Humans of DataHub.
            </p>
          </>
        ),        
        social: {
          linkedin: "https://www.linkedin.com/in/li-haihui",
          github: "https://github.com/HarveyLeo",
          },
        location: "Singapore"
        },
      {
        name: "Fredrik Sannholm",
        position: "Wolt",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/fs_black.jpg",
        position: "",
        bio: (
          <>
            <p>
            Drove DataHub adoption at Wolt and featured in Humans of DataHub.           
            </p>
          </>
        ),        
        social: {
          },
        location: "Finland"
        },
        {
          name: "Scott Maciej",
          position: "Optum",
          image: "https://raw.githubusercontent.com/datahub-project/static-assets/champ-img/imgs/datahub-champions/initials/sm.jpg",
          bio: (
            <>
              <p>
              Drove DataHub's adaptation and implementation at Optum.
              </p>
            </>
          ),
          social: {
            linkedin: "https://www.linkedin.com/in/scottmaciej/",
            github: "https://github.com/sgm44",
            web: "https://www.badhabitbeer.com/",
            },
          location: "USA"
      },
      {
        name: "Tim Bossenmaier",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/champ-img/imgs/datahub-champions/tim_bossenmaier.jpeg",
        position: "Data & Software Engineer, Cloudflight",
        bio: (
          <>
            <p>
            Reliably provides direction to community members and submitted 5 pull request, including improvements to Athena ingestion (support for nested schemas) and the REST emitter. 
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
        name: "Nikola Kasev",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/champ-img/imgs/datahub-champions/nikola_kasev.jpeg",
        position: "Data Engineer, KPN",
        bio: (
          <>
            <p>
            Reliably provided direction to Community Members across all support channels in Slack.
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
        position: "Coursera",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/datahub-champions/initials/nn.jpg",
        bio: (
          <>
            <p>
              Drove DataHub's adaptation and implementation on Coursera.
            </p>
          </>
        ),
        social: {
          },
        },
      {
        name: "Wu Teng",
        position: "CashApp",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/datahub-champions/initials/wt.jpg",
        bio: (
          <>
            <p>
              Reliably provided direction to Community Members across all support channels in Slack.
            </p>
          </>
        ),
        social: {
          },
        location: "Australia"
      },
      {
        name: "Felipe Gusmao",
        position: "Zynga",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/datahub-champions/initials/fg.jpg",
        bio: (
          <>
            <p>
            Shared Zynga's experience adopting and implementing DataHub during the September 2023 Town Hall.
            </p>
          </>
        ),
        social: {
          },
        location: "Toronto, Canada"
       },
       {
        name: "Sudhakara ST",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/datahub-champions/initials/ss.jpg",
        position: "Engineer, Zynga",
        bio: (
          <>
            <p>
            Reliably provided direction to Community Members across all support channels in Slack and shared Zynga's experience adopting and implementing DataHub during the September 2023 Town Hall.
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
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/datahub-champions/initials/bn.jpg",
        position: "Technical Product Manager,	Optum ",
        bio: (
          <>
            <p>
            Drove DataHub's adaptation and implementation at Optum.
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
        position: "Optum",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/datahub-champions/initials/dk.jpg",
        bio: (
          <>
            <p>
            Drove DataHub's adaptation and implementation at Optum.
            </p>
          </>
        ),
        social: {
          },
        location: "USA"
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
        name: "Kate Koy",
        position: "Optum",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/datahub-champions/initials/kk.jpg",
        bio: (
          <>
            <p>
            Drove DataHub's adaptation and implementation at Optum.
            </p>
          </>
        ),
        social: {
          },
        location: "USA"
      },
      {
        name: "Anjali Arora",
        position: "Optum",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/datahub-champions/initials/aa.jpg",
        bio: (
          <>
            <p>
            Drove DataHub's adaptation and implementation at Optum.
            </p>
          </>
        ),
        social: {
          },
          location: "USA"
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
