import React from 'react';
import styles from './styles.module.scss';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import solutionsDropdownContent from './solutionsDropdownContent'; 

function SolutionsDropdownContent() {
  const { fullSizeCards, halfSizeCards } = solutionsDropdownContent;

  return (
    <div className={clsx(styles.container)}>
      <div className={clsx(styles.row)}>
        {/* Full-size cards */}
        {fullSizeCards.map((item, index) => (
          <div key={`full-${index}`} className={clsx(styles.col)}>
            <Link
              className={clsx(styles.card, styles.fullSizeCard)}
              to={item.href}
            >
              <div className={clsx(styles.cardContent)}>
                <div className={clsx(styles.cardText)}>
                  <img
                      src={item.iconImage}
                      alt={item.title}
                      className={clsx(styles.icon)}
                  />
                  <div className={clsx(styles.header)}>
                    <div className={clsx(styles.title)}>{item.title}</div>
                  </div>
                  <div className={clsx(styles.description)}>{item.description}</div>
                </div>
              </div>
            </Link>
          </div>
        ))}

        {/* Half-size cards */}
        <div className={clsx(styles.col, styles.halfSizeWrapper)}>
          {halfSizeCards.map((item, index) => (
            <div key={`half-${index}`} className={clsx(styles.halfSizeCardWrapper)}>
              <Link
                className={clsx(styles.card, styles.halfSizeCard)}
                to={item.href}
              >
                <div className={clsx(styles.cardContent)}>
                  <div className={clsx(styles.cardText)}>
                    <div className={clsx(styles.header)}>
                      <img
                        src={item.iconImage}
                        alt={item.title}
                        className={clsx(styles.icon)}
                      />
                      <div className={clsx(styles.title)}>{item.title}</div>
                    </div>
                    <div className={clsx(styles.description)}>{item.description}</div>
                  </div>
                </div>
              </Link>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

export default SolutionsDropdownContent;
