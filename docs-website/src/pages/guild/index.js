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
        name: "Aezo Teo",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/aezo-teo.jpg",
        bio: "Data Engineer, Grab",
        social: {
          linkedin: "https://www.linkedin.com/in/aezomz",
          twitter: "https://twitter.com/morning_teofee",
          github: "https://github.com/aezomz",
        },
      },
      {
        name: "Arun Vasudevan",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/arun-vasudevan.jpg",
        bio: "Staff Software Engineer, Peloton",
        social: {
          linkedin: "https://www.linkedin.com/in/arun-vasudevan-55117368/",
          github: "https://github.com/arunvasudevan",
        },
      },
      {
        name: "Boyuan Zhang",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/bz_white.jpg",
        bio: "Data Engineer, Credit Karma",
        social: {
          linkedin: "https://www.linkedin.com/in/bbbzhang",
          },
        },
      {
        name: "Bumsoo Kim",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/bumsoo-kim.jpg",
        bio: "Software Engineer",
        social: {
          linkedin: "https://www.linkedin.com/in/bumsoo",
          github: "https://github.com/bskim45",
          },
        },
      {
        name: "David Haglund",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/david-haglund.jpeg",
        bio: "Data Engineer, SSAB",
        social: {
          github: "https://github.com/daha",
          },
        },
      {
        name: "David Sánchez",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/david-sanchez.jpg",
        bio: "Principal Data Engineer, Cabify",
        social: {
          },
        },
      {
        name: "Djordje Mijatovic",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/dm_white.jpg",
        bio: "Senior Java Developer",
        social: {
          linkedin: "https://www.linkedin.com/in/djordje-mijatovic-aa22bb76/",
          },
        },
      {
        name: "Ebu",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/e_white.jpg",
        bio: "Core staff, KDDI",
        social: {
          github: "https://github.com/eburairu",
          },
        },
      {
        name: "Eric Ladouceur",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/el_white.jpg",
        bio: "Technical Advisor, Canadian Centre for Cyber Security",
        social: {
          github: "https://github.com/cccs-eric",
          },
        },
      {
        name: "Felix Lüdin",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/fl_white.jpg",
        bio: "Process Consultant Business Analytics, Endress+Hauser",
        social: {
          linkedin: "https://www.linkedin.com/in/felix-l%C3%BCdin-222304209/",
          github: "https://github.com/Masterchen09",
          },
        },
      {
        name: "Jordan Wolinsky",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/jordan-wolinsky.jpeg",
        bio: "Senior Software Engineer, Zephyr AI",
        social: {
          linkedin: "https://www.linkedin.com/in/jordan-wolinsky/",
          github: "https://github.com/jiafi",
          },
        },
      {
        name: "Marcin Szymanski",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/marcin-szymanski.JPG",
        bio: "Data Engineering Manager, Esure",
        social: {
          linkedin: "https://www.linkedin.com/in/marcinszymanskipl/",
          github: "https://github.com/ms32035",
          web: "www.marcinszymanski.pl",
        },
      },
        {
        name: "Mert Tunc",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/mert-tunc.png",
        bio: "Staff Software Engineer, Udemy",
        social: {
          linkedin: "https://www.linkedin.com/in/merttunc96/",
          },
        },
        {
        name: "Mike Schlosser",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/mike-schlosser.jpeg",
        bio: "Lead Software Engineer, Kyros",
        social: {
          linkedin: "https://www.linkedin.com/in/michael-schlosser",
          twitter: "https://twitter.com/Mikeschlosser16",
          github: "https://github.com/Mikeschlosser16",
          },
        },
        {
        name: "Parham Ghazanfari",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/pg_white.jpg",
        bio: "Software Development Engineer, Amazon",
        social: {
          linkedin: "https://www.linkedin.com/in/parham-ghazanfari-a8b40b89/",
          mastodon: "https://mastodon.social/",
          github: "https://github.com/pghazanfari",
          },
        },
        {
        name: "Piotr Skrydalewicz",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/ps_white.jpg",
        bio: "Data Engineering Consultant",
        social: {
          },
        },
        {
        name: "Xu Wang",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/xu-wang.jpeg",
        bio: "Staff Software Engineer, Included Health",
        social: {
          },
        },
    ],
  },
  {
    name: "Community Supporter",
    description: "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nulla venenatis libero enim, eu porta tortor faucibus eu.",
    badge: "/img/guild/badge-community-supporter.svg",
    people: [
      {
        name: "Mohamad Amzar",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/amz.jpeg",
        bio: "Analytic Engineer, CDX",
        social: {
          github: "https://github.com/amzar96",
        },
      },
      {
        name: "Nguyen Tri Hieu",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/hieu-nguyen-tri-hieu.jpg",
        bio: "Data Engineer, Fossil Vietnam",
        social: {
          linkedin: "https://www.linkedin.com/in/hieunguyen-it/",
        },
      },
      {
        name: "Nicholas Shook",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/ns_yellow.jpg",
        bio: "Platform Engineer",
        social: {
          web: "HTTPS://shook.family",
        },
      },
      {
        name: "Pablo Ochoa",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/pablo-ochoa-ajamil.jpg",
        bio: "Big Data and Data Governance Consultant, Graphenus",
        social: {
          linkedin: "https://www.linkedin.com/in/pablo-ochoa-ajamil-61037a211/",
        },
      },
      {
        name: "Patrick Braz",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/patrick-franco-braz.jpeg",
        bio: "Data Engineering, Hurb",
        social: {
          linkedin: "https://www.linkedin.com/in/patrick-franco-braz/",
          github: "https://github.com/PatrickfBraz",
        },
      },
      {
        name: "Pedro Aguiar",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/pa_yellow.jpg",
        bio: "Data Analyst",
        social: {
          linkedin: "https://www.linkedin.com/in/pdraguiar",
          github: "https://github.com/pdraguiar",
          web: "http://www.pedroaguiar.com.br/",
        },
      },
      {
        name: "Steve Pham",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/cuong-pham.jpeg",
        bio: "Principal Engineer",
        social: {
          linkedin: "https://www.linkedin.com/in/steve-pham",
        },
      },
      {
        name: "Xianglong LIU",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/xl_yellow.jpg",
        bio: "Data Platform Engineer",
        social: {
        },
      },
    ],
  },
  {
    name: "DataHub Champion",
    description: "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nulla venenatis libero enim, eu porta tortor faucibus eu.",
    badge: "/img/guild/badge-datahub-champion.svg",
    people: [
      {
        name: "Abhishek Sharma",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/as_black.jpg",
        bio: "Software Engineer",
        social: {
        },
      },
      {
        name: "Alexander Dobrev",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/alex-dobrev.jpg",
        bio: "Product Manager, Grab",
        social: {
          linkedin: "https://www.linkedin.com/in/adobrev/",
        },
      },
      {
        name: "Amanda Ng",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/amanda-ng.png",
        bio: "Senior Software Engineer, Grab",
        social: {
          linkedin: "https://sg.linkedin.com/in/amandang19",
          github: "https://github.com/ngamanda",
          web: "https://ngamanda.com",
        },
      },
      {
        name: "Atul Saurav",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/atul-saurav.png",
        bio: "Data Governance Architect",
        social: {
          linkedin: "https://linkedIn.com/in/atulsaurav",
          twitter: "https://twitter.com/twtAtul",
        },
      },
      {
        name: "Divya Manohar",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/divya-manohar.png",
        bio: "Software Engineer, Stripe",
        social: {
          linkedin: "https://www.linkedin.com/in/divya-manohar-20862716a/",
        },
      },
      {
        name: "Edward Vaisman",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/edward-vaisman.png",
        bio: "Staff Customer Innovation Engineer, Confluent",
        social: {
          linkedin: "https://www.linkedin.com/in/edwardvaisman",
          github: "https://github.com/eddyv",
          web: "https://edwardvaisman.ca",
        },
      },
      {
        name: "Eric Cooklin",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/ec_black.jpg",
        bio: "Sr Data Engineer, Stash",
        social: {
          linkedin: "https://www.linkedin.com/in/eric-cooklin-3b63a1129",
        },
      },
      {
        name: "Fredrik Sannholm",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/fs_black.jpg",
        bio: "Staff Engineer",
        social: {
          linkedin: "https://www.linkedin.com/in/fredriksannholm",
        },
      },
      {
        name: "Gary Stafford",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/gary-stafford.JPG",
        bio: "Principal Solutions Architect/Analytics TFC, AWS",
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
        bio: "Lead Data Enginee, Grab",
        social: {
          linkedin: "https://www.linkedin.com/in/li-haihui",
          github: "https://github.com/HarveyLeo",
        },
      },
      {
        name: "Hyejin Yoon",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/hyejin-yoon.jpg",
        bio: "Data Engineer, SOCAR",
        social: {
          linkedin: "https://www.linkedin.com/in/hyejinyoon/",
        },
      },
      {
        name: "Imane Lafnoune",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/imane-lafnoune.jpeg",
        bio: "Data Engineer, Sicara",
        social: {
          linkedin: "https://www.linkedin.com/in/imanelafnoune/",
          twitter: "https://twitter.com/ImaneLafn",
          github: "https://github.com/imlaf",
        },
      },
      {
        name: "Kartik Darapuneni",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/kd_black.jpg",
        bio: "Software Engineer, Included Health",
        social: {
        },
      },

      {
        name: "Liangjun Jiang",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/lj_black.jpg",
        bio: "Software Engineering Manager",
        social: {
        },
      },
      {
        name: "Mike Linthe",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/mike-linthe.jpeg",
        bio: "COO, Contiamo",
        social: {
          linkedin: "https://www.linkedin.com/in/mike-linthe/",
          web: "www.contiamo.com",
        },
      },
      {
        name: "Nidhin Nandhakumar",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/nidhin-nandhakumar.jpeg",
        bio: "Senior Data Engineer, Coursera",
        social: {
          linkedin: "https://www.linkedin.com/in/nidhin-nandhakumar-874ba885/",
        },
      },
      {
        name: "Sergio Gómez",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/sergio-gomez-villamor.jpeg",
        bio: "Technical Lead, Adevinta",
        social: {
          linkedin: "https://www.linkedin.com/in/sgomezvillamor/",
          twitter: "https://twitter.com/sgomezvillamor",
          github: "https://github.com/sgomezvillamor",
        },
      },
      {
        name: "Steven Po",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/steven-zhi-wai-po.jpeg",
        bio: "Senior Data Engineer, Coursera",
        social: {
          linkedin: "https://www.linkedin.com/in/steven-zhi-wai-po-12a28626/",
        },
      },
      {
        name: "Vishal Shah",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/vishal-shah.jpg",
        bio: "Senior Software Engineer, Etsy",
        social: {
          linkedin: "https://www.linkedin.com/in/vishal-c-shah",
        },
      },
      {
        name: "Zhong Xu",
        image: "https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/data-practitioners-guild/initials/zx_black.jpg",
        bio: "Software Engineer, Pinterest",
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
