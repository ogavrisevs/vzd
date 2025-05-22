DROP TABLE IF EXISTS aw_merge;
CREATE TABLE IF NOT EXISTS aw_merge (
    adr varchar(256),
    kods int,
    tips smallint,
    statuss varchar(3),
    dziv varchar(128), 
    eka varchar(128), 
    iela varchar(128), 
    ciems varchar(128), 
    pilseta varchar(128), 
    pagasts varchar(128), 
    novads varchar(128), 
    rajons varchar(128),
    pasta_kods varchar(7),
    UNIQUE(kods, tips)
);

DROP index IF EXISTS aw_dziv_index;
DROP index IF EXISTS aw_eka_index;
DROP index IF EXISTS aw_iela_index;
DROP index IF EXISTS aw_pilseta_index;
DROP index IF EXISTS aw_ciems_index;

CREATE INDEX aw_dziv_index ON aw_dziv(KODS, TIPS_CD);
CREATE INDEX aw_eka_index ON aw_eka(KODS, TIPS_CD);
CREATE INDEX aw_iela_index ON aw_iela(KODS, TIPS_CD);
CREATE INDEX aw_pilseta_index ON aw_pilseta(KODS, TIPS_CD);
CREATE INDEX aw_ciems_index ON aw_ciems(KODS, TIPS_CD);