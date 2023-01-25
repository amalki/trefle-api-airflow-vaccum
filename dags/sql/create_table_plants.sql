CREATE OR REPLACE TABLE PLANTS(
   id                 INTEGER  NOT NULL PRIMARY KEY 
  ,common_name        VARCHAR(255)
  ,slug               VARCHAR(255)
  ,scientific_name    VARCHAR(255)
  ,year               INTEGER 
  ,bibliography       VARCHAR(1000)
  ,author             VARCHAR(255)
  ,status             VARCHAR(255)
  ,rank               VARCHAR(255)
  ,family_common_name VARCHAR(255)
  ,genus_id           INTEGER 
  ,image_url          VARCHAR(255)
  ,synonyms           VARCHAR(100000)
  ,genus              VARCHAR(255)
  ,family             VARCHAR(255)
  ,links              VARCHAR(255)
);