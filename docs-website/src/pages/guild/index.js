import React from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import { useColorMode } from "@docusaurus/theme-common";

import GuildSection from "./_components/GuildSection";

const guildSections = [
  {
    name: "Top Code Contributor",
    description: "",
    alttext: "Building high-impact features and integrations within DataHub",
    badge: "/img/guild/badge-top-code-contributor.svg",
    people: [
      {
        name: "Aezo Teo",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/aezo-teo.jpg",
        bio: (
          <>
            <b>Data Engineer, Grab</b>
            <p>
              <p> </p>
              Sateful ingestion for Presto-Hive connector, 
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/aezomz",
          twitter: "https://twitter.com/morning_teofee",
          github: "https://github.com/aezomz",
        },
      },
      {
        name: "Arun Vasudevan",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/arun-vasudevan.jpg",
        bio: (
          <>
            <b>Staff Software Engineer, Peloton</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/arun-vasudevan-55117368/",
          github: "https://github.com/arunvasudevan",
        },
      },
      {
        name: "Boyuan Zhang",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/bz_white.jpg",
        bio: (
          <>
            <b>Data Engineer, Credit Karma</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/bbbzhang",
          },
        },
      {
        name: "Bumsoo Kim",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/bumsoo-kim.jpg",
        bio: (
          <>
            <b>Software Engineer</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/bumsoo",
          github: "https://github.com/bskim45",
          },
        },
      {
        name: "David Haglund",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/david-haglund.jpeg",
        bio: (
          <>
            <b>Data Engineer, SSAB</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),        
        social: {
          github: "https://github.com/daha",
          },
        },
      {
        name: "David Sánchez",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/david-sanchez.jpg",      
        bio: (
          <>
            <b>Principal Data Engineer, Cabify</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),        
        social: {
          },
        },
      {
        name: "Djordje Mijatovic",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/dm_white.jpg",
        bio: (
          <>
            <b>Senior Java Developer</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/djordje-mijatovic-aa22bb76/",
          },
        },
      {
        name: "Ebu",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/e_white.jpg",
        bio: (
          <>
            <b>Core Staff, KDDI</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          github: "https://github.com/eburairu",
          },
        },
      {
        name: "Eric Ladouceur",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/el_white.jpg",
        bio: (
          <>
            <b>Technical Advisor, Canadian Centre for Cyber Security</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          github: "https://github.com/cccs-eric",
          },
        },
      {
        name: "Felix Lüdin",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/fl_white.jpg",
        bio: (
          <>
            <b>Process Consultant Business Analytics, Endress+Hauser</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/felix-l%C3%BCdin-222304209/",
          github: "https://github.com/Masterchen09",
          },
        },
      {
        name: "Jordan Wolinsky",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/jordan-wolinsky.jpeg",
        bio: (
          <>
            <b>Senior Software Engineer, Zephyr AI</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/jordan-wolinsky/",
          github: "https://github.com/jiafi",
          },
        },
      {
        name: "Marcin Szymanski",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/marcin-szymanski.JPG",
        bio: (
          <>
            <b>Data Engineering Manager, Esure</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/marcinszymanskipl/",
          github: "https://github.com/ms32035",
          web: "www.marcinszymanski.pl",
        },
      },
        {
        name: "Mert Tunc",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/mert-tunc.png",
        bio: (
          <>
            <b>Staff Software Engineer, Udemy</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/merttunc96/",
          },
        },
        {
        name: "Mike Schlosser",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/mike-schlosser.jpeg",
        bio: (
          <>
            <b>Lead Software Engineer, Kyros</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/michael-schlosser",
          twitter: "https://twitter.com/Mikeschlosser16",
          github: "https://github.com/Mikeschlosser16",
          },
        },
        {
        name: "Parham Ghazanfari",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/pg_white.jpg",
        bio: (
          <>
            <b>Software Development Engineer, Amazon</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ), 
        social: {
          linkedin: "https://www.linkedin.com/in/parham-ghazanfari-a8b40b89/",
          mastodon: "https://mastodon.social/",
          github: "https://github.com/pghazanfari",
          },
        },
        {
        name: "Piotr Skrydalewicz",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/ps_white.jpg",
        bio: (
          <>
            <b>Data Engineering Consultant</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          },
        },
        {
        name: "Xu Wang",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/xu-wang.jpeg",
        bio: (
          <>
            <b>Staff Software Engineer, Included Health</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          },
        },
    ],
  },
  {
    name: "Community Supporter",
    description:  "",
    alttext: "Helping Community Members succeed in implementing DataHub", 
    badge: "/img/guild/badge-community-supporter.svg",
    people: [
      {
        name: "Mohamad Amzar",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/amz.jpeg",
        bio: (
          <>
            <b>Analytics Engineer, CDX</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          github: "https://github.com/amzar96",
        },
      },
      {
        name: "Nguyen Tri Hieu",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/hieu-nguyen-tri-hieu.jpg",
        bio: (
          <>
            <b>Data Engineer, Fossil Vietnam</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/hieunguyen-it/",
        },
      },
      {
        name: "Nicholas Shook",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/ns_yellow.jpg",
        bio: (
          <>
            <b>Platform Engineer</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          web: "HTTPS://shook.family",
        },
      },
      {
        name: "Pablo Ochoa",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/pablo-ochoa-ajamil.jpg",
        bio: (
          <>
            <b>Big Data and Data Governance Consultant, Graphenus</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/pablo-ochoa-ajamil-61037a211/",
        },
      },
      {
        name: "Patrick Braz",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/patrick-franco-braz.jpeg",
        bio: (
          <>
            <b>Data Engineering, Hurb</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/patrick-franco-braz/",
          github: "https://github.com/PatrickfBraz",
        },
      },
      {
        name: "Pedro Aguiar",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/pa_yellow.jpg",
        bio: (
          <>
            <b>Data Analyst</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/pdraguiar",
          github: "https://github.com/pdraguiar",
          web: "http://www.pedroaguiar.com.br/",
        },
      },
      {
        name: "Steve Pham",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/cuong-pham.jpeg",
        bio: (
          <>
            <b>Principal Engineer</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/steve-pham",
        },
      },
      {
        name: "Xianglong LIU",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/xl_yellow.jpg",
        bio: (
          <>
            <b>Data Platform Engineer</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
        },
      },
    ],
  },
  {
    name: "DataHub Champion",
    description:  "",
    alttext: "Amplifying awareness and adoption of DataHub",
    badge: "/img/guild/badge-datahub-champion.svg",
    people: [
      {
        name: "Abhishek Sharma",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/as_black.jpg",
        bio: (
          <>
            <b>Software Engineer</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
        },
      },
      {
        name: "Alexander Dobrev",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/alex-dobrev.jpg",
        bio: (
          <>
            <b>Product Manager, Grab</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/adobrev/",
        },
      },
      {
        name: "Amanda Ng",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/amanda-ng.png",
        bio: (
          <>
            <b>Senior Software Engineer, Grab</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://sg.linkedin.com/in/amandang19",
          github: "https://github.com/ngamanda",
          web: "https://ngamanda.com",
        },
      },
      {
        name: "Atul Saurav",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/atul-saurav.png",
        bio: (
          <>
            <b>Data Governance Architect</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://linkedIn.com/in/atulsaurav",
          twitter: "https://twitter.com/twtAtul",
        },
      },
      {
        name: "Divya Manohar",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/divya-manohar.png",      
        bio: (
          <>
            <b>Software Engineer, Stripe</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/divya-manohar-20862716a/",
        },
      },
      {
        name: "Edward Vaisman",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/edward-vaisman.png",
        bio: (
          <>
            <b>Staff Customer Innovation Engineer, Confluent</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/edwardvaisman",
          github: "https://github.com/eddyv",
          web: "https://edwardvaisman.ca",
        },
      },
      {
        name: "Eric Cooklin",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/ec_black.jpg",
        bio: (
          <>
            <b>Sr Data Engineer, Stash</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/eric-cooklin-3b63a1129",
        },
      },
      {
        name: "Fredrik Sannholm",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/fs_black.jpg",
        bio: (
          <>
            <b>Staff Engineer</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/fredriksannholm",
        },
      },
      {
        name: "Gary Stafford",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/gary-stafford.JPG",
        bio: (
          <>
            <b>Principal Solutions Architect/Analytics TFC, AWS</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/garystafford/",
          twitter: "https://twitter.com/GaryStafford",
          github: "https://github.com/garystafford",
          web: "https://medium.com/@GaryStafford",
        },
      },
      {
        name: "Harvey Li",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/hl_black.jpg",
        bio: (
          <>
            <b>Lead Data Engineer, Grab</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/li-haihui",
          github: "https://github.com/HarveyLeo",
        },
      },
      {
        name: "Hyejin Yoon",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/hyejin-yoon.jpg",
        bio: (
          <>
            <b>Data Engineer, SOCAR</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/hyejinyoon/",
        },
      },
      {
        name: "Imane Lafnoune",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/imane-lafnoune.jpeg",
        bio: (
          <>
            <b>Data Engineer, Sicara</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/imanelafnoune/",
          twitter: "https://twitter.com/ImaneLafn",
          github: "https://github.com/imlaf",
        },
      },
      {
        name: "Kartik Darapuneni",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/kd_black.jpg",
        bio: (
          <>
            <b>Software Engineer, Included Health</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
        },
      },

      {
        name: "Liangjun Jiang",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/lj_black.jpg",
        bio: (
          <>
            <b>Software Engineering Manager</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
        },
      },
      {
        name: "Mike Linthe",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/mike-linthe.jpeg",
        bio: (
          <>
            <b>COO, Contiamo</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/mike-linthe/",
          web: "www.contiamo.com",
        },
      },
      {
        name: "Nidhin Nandhakumar",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/nidhin-nandhakumar.jpeg",
        bio: (
          <>
            <b>Senior Data Engineer, Coursera</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/nidhin-nandhakumar-874ba885/",
        },
      },
      {
        name: "Sergio Gómez",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/sergio-gomez-villamor.jpeg",
        bio: (
          <>
            <b>Technical Lead, Adevinta</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/sgomezvillamor/",
          twitter: "https://twitter.com/sgomezvillamor",
          github: "https://github.com/sgomezvillamor",
        },
      },
      {
        name: "Steven Po",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/steven-zhi-wai-po.jpeg",
        bio: (
          <>
            <b>Senior Data Engineer, Coursera</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/steven-zhi-wai-po-12a28626/",
        },
      },
      {
        name: "Vishal Shah",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/vishal-shah.jpg",
        bio: (
          <>
            <b>Senior Software Engineer, Etsy</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
          linkedin: "https://www.linkedin.com/in/vishal-c-shah",
        },
      },
      {
        name: "Zhong Xu",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/zx_black.jpg",
        bio: (
          <>
            <b>Software Engineer, Pinterest</b>
            <p>
              <p> </p>
              Summary of contribution
            </p>
          </>
        ),
        social: {
        },
      },
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
                Celebrating community members that have gone above and beyond to contribute to the collective success of DataHub
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
