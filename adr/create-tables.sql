CREATE TABLE IF NOT EXISTS aw_ciems (
    KODS int,               -- Adresācijas objekta kods, Number (9)
    TIPS_CD smallint,       -- Adresācijas objekta tipa kods, Number (3)
    NOSAUKUMS varchar(128), -- Adresācijas objekta nosaukums, Varchar2 (128)
    VKUR_CD int,            -- Tā adresācijas objekta kods, kuram hierarhiski pakļauts attiecīgais adresācijas objekts, Number (9)
    VKUR_TIPS smallint,     -- Tā adresācijas objekta tipa kods, kuram hierarhiski pakļauts attiecīgais adresācijas objekts, Number (3)
    APSTIPR varchar(1),  
    APST_PAK smallint,  
    STATUSS varchar(3),     -- Adresācijas objekta statuss: DEL – likvidēts, Varchar2 (3)
    SORT_NOS varchar(516),  -- Kārtošanas nosacījums adresācijas objekta nosaukumam, Varchar2 (516)
    DAT_SAK varchar(32),
    DAT_MOD varchar(32),
    DAT_BEIG varchar(32),
    ATRIB varchar(32), 
    STD varchar(256)        -- Adresācijas objekta pilnais adreses pierakst, Varchar2 (256)
);

CREATE TABLE IF NOT EXISTS aw_dziv (
    KODS int,               -- Adresācijas objekta kods, Number (9)
    TIPS_CD smallint,       -- Adresācijas objekta tipa kods, Number (3)
    STATUSS varchar(3),     -- Adresācijas objekta statuss: DEL – likvidēts, Varchar2 (3)
    APSTIPR varchar(1),  
    APST_PAK smallint,  
    VKUR_CD int,            -- Tā adresācijas objekta kods, kuram hierarhiski pakļauts attiecīgais adresācijas objekts, Number (9)
    VKUR_TIPS smallint,     -- Tā adresācijas objekta tipa kods, kuram hierarhiski pakļauts attiecīgais adresācijas objekts, Number (3)
    NOSAUKUMS varchar(128), -- Adresācijas objekta nosaukums, Varchar2 (128)
    SORT_NOS varchar(516),  -- Kārtošanas nosacījums adresācijas objekta nosaukumam, Varchar2 (516)
    ATRIB varchar(32), 
    DAT_SAK varchar(256),
    DAT_MOD varchar(256),
    DAT_BEIG varchar(256),
    STD varchar(256)        -- Adresācijas objekta pilnais adreses pierakst, Varchar2 (256)
);

CREATE TABLE IF NOT EXISTS aw_eka (
    KODS int,               -- Adresācijas objekta kods, Number (9)
    TIPS_CD smallint,       -- Adresācijas objekta tipa kods, Number (3)
    STATUSS varchar(3),     -- Adresācijas objekta statuss: DEL – likvidēts, Varchar2 (3)
    APSTIPR varchar(1),  
    APST_PAK smallint,  
    VKUR_CD int,            -- Tā adresācijas objekta kods, kuram hierarhiski pakļauts attiecīgais adresācijas objekts, Number (9)
    VKUR_TIPS smallint,     -- Tā adresācijas objekta tipa kods, kuram hierarhiski pakļauts attiecīgais adresācijas objekts, Number (3)
    NOSAUKUMS varchar(128), -- Adresācijas objekta nosaukums, Varchar2 (128)
    SORT_NOS varchar(516),  -- Kārtošanas nosacījums adresācijas objekta nosaukumam, Varchar2 (516)
    ATRIB varchar(32), 
    PNOD_CD varchar(256),
    DAT_SAK varchar(32),
    DAT_MOD varchar(32),
    DAT_BEIG varchar(32),
    FOR_BUILD varchar(1),   -- Pazīme, ka adresācijas objekts ir apbūvei paredzēta zemes vienība. Vērtība “Y” – ir apbūvei paredzēta zemes vienība; vērtība “N” – ir ēka
    PLAN_ADR  varchar(10),  -- Pazīme, ka tā ir plānota adrese. Vērtība “Y” – plānotā adrese (adrese nav piesaistīta nevienam objektam Kadastra informācijas sistēmā); vērtība “N” – plānotā adrese ir piesaistīta nekustamā īpašuma objektam Kadastra informācijas sistēmā
    STD varchar(256),       -- Adresācijas objekta pilnais adreses pierakst, Varchar2 (256)
    KOORD_X real,           -- Adresācijas objekta centroīda X koordināte, LKS-92, Number (9,3) 
    KOORD_Y real,           -- Adresācijas objekta centroīda Y koordināte, LKS-92, Number (9,3) 
    DD_N real,              -- Adresācijas objekta centroīda koordināte DD_E, Number (2,7)
    DD_E real               -- Adresācijas objekta centroīda koordināte DD_N, Number (2,7)
);

CREATE TABLE IF NOT EXISTS aw_iela (
    KODS int,               -- Adresācijas objekta kods, Number (9)
    TIPS_CD smallint,       -- Adresācijas objekta tipa kods, Number (3)
    NOSAUKUMS varchar(128), -- Adresācijas objekta nosaukums, Varchar2 (128)
    VKUR_CD int,            -- Tā adresācijas objekta kods, kuram hierarhiski pakļauts attiecīgais adresācijas objekts, Number (9)
    VKUR_TIPS smallint,     -- Tā adresācijas objekta tipa kods, kuram hierarhiski pakļauts attiecīgais adresācijas objekts, Number (3)
    APSTIPR varchar(1),  
    APST_PAK smallint,  
    STATUSS varchar(3),     -- Adresācijas objekta statuss: DEL – likvidēts, Varchar2 (3)
    SORT_NOS varchar(516),  -- Kārtošanas nosacījums adresācijas objekta nosaukumam, Varchar2 (516)
    DAT_SAK varchar(32),
    DAT_MOD varchar(32),
    DAT_BEIG varchar(32),
    ATRIB varchar(32), 
    STD varchar(256)        -- Adresācijas objekta pilnais adreses pierakst, Varchar2 (256)
);

CREATE TABLE IF NOT EXISTS aw_novads (
    KODS int,               -- Adresācijas objekta kods, Number (9)
    TIPS_CD smallint,       -- Adresācijas objekta tipa kods, Number (3)
    NOSAUKUMS varchar(128), -- Adresācijas objekta nosaukums, Varchar2 (128)
    VKUR_CD int,            -- Tā adresācijas objekta kods, kuram hierarhiski pakļauts attiecīgais adresācijas objekts, Number (9)
    VKUR_TIPS smallint,     -- Tā adresācijas objekta tipa kods, kuram hierarhiski pakļauts attiecīgais adresācijas objekts, Number (3)
    APSTIPR varchar(1),  
    APST_PAK smallint,  
    STATUSS varchar(3),     -- Adresācijas objekta statuss: DEL – likvidēts, Varchar2 (3)
    SORT_NOS varchar(516),  -- Kārtošanas nosacījums adresācijas objekta nosaukumam, Varchar2 (516)
    DAT_SAK varchar(32),
    DAT_MOD varchar(32),
    DAT_BEIG varchar(32),
    ATRIB varchar(32), 
    STD varchar(256)        -- Adresācijas objekta pilnais adreses pierakst, Varchar2 (256)
);

CREATE TABLE IF NOT EXISTS aw_pagasts (
    KODS int,               -- Adresācijas objekta kods, Number (9)
    TIPS_CD smallint,       -- Adresācijas objekta tipa kods, Number (3)
    NOSAUKUMS varchar(128), -- Adresācijas objekta nosaukums, Varchar2 (128)
    VKUR_CD int,            -- Tā adresācijas objekta kods, kuram hierarhiski pakļauts attiecīgais adresācijas objekts, Number (9)
    VKUR_TIPS smallint,     -- Tā adresācijas objekta tipa kods, kuram hierarhiski pakļauts attiecīgais adresācijas objekts, Number (3)
    APSTIPR varchar(1),  
    APST_PAK smallint,  
    STATUSS varchar(3),     -- Adresācijas objekta statuss: DEL – likvidēts, Varchar2 (3)
    SORT_NOS varchar(516),  -- Kārtošanas nosacījums adresācijas objekta nosaukumam, Varchar2 (516)
    DAT_SAK varchar(32),
    DAT_MOD varchar(32),
    DAT_BEIG varchar(32),
    ATRIB varchar(32), 
    STD varchar(256)        -- Adresācijas objekta pilnais adreses pierakst, Varchar2 (256)
);

CREATE TABLE IF NOT EXISTS aw_pilseta (
    KODS int,               -- Adresācijas objekta kods, Number (9)
    TIPS_CD smallint,       -- Adresācijas objekta tipa kods, Number (3)
    NOSAUKUMS varchar(128), -- Adresācijas objekta nosaukums, Varchar2 (128)
    VKUR_CD int,            -- Tā adresācijas objekta kods, kuram hierarhiski pakļauts attiecīgais adresācijas objekts, Number (9)
    VKUR_TIPS smallint,     -- Tā adresācijas objekta tipa kods, kuram hierarhiski pakļauts attiecīgais adresācijas objekts, Number (3)
    APSTIPR varchar(1),  
    APST_PAK smallint,  
    STATUSS varchar(3),     -- Adresācijas objekta statuss: DEL – likvidēts, Varchar2 (3)
    SORT_NOS varchar(516),  -- Kārtošanas nosacījums adresācijas objekta nosaukumam, Varchar2 (516)
    DAT_SAK varchar(32),
    DAT_MOD varchar(32),
    DAT_BEIG varchar(32),
    ATRIB varchar(32), 
    STD varchar(256)        -- Adresācijas objekta pilnais adreses pierakst, Varchar2 (256)
);

CREATE TABLE IF NOT EXISTS aw_ppils (
    KODS int,           -- Adresācijas objekta kods ēkai., Number (9)
    PPILS varchar(256)  -- Priekšpilsētas nosaukums, Varchar2 (32)
);

CREATE TABLE IF NOT EXISTS aw_rajons (
    KODS int,               -- Adresācijas objekta kods, Number (9)
    TIPS_CD smallint,       -- Adresācijas objekta tipa kods, Number (3)
    NOSAUKUMS varchar(128), -- Adresācijas objekta nosaukums, Varchar2 (128)
    VKUR_CD int,            -- Tā adresācijas objekta kods, kuram hierarhiski pakļauts attiecīgais adresācijas objekts, Number (9)
    VKUR_TIPS smallint,     -- Tā adresācijas objekta tipa kods, kuram hierarhiski pakļauts attiecīgais adresācijas objekts, Number (3)
    APSTIPR varchar(1),     -- Vērtība “Y” norāda vai adresācijas objekts ir apstiprināts, Varchar2 (1)
    APST_PAK smallint,      -- Adresācijas objekta apstiprinājuma pakāpe, Number (3)
    STATUSS varchar(3),     -- Adresācijas objekta statuss: DEL – likvidēts, Varchar2 (3)
    SORT_NOS varchar(516),  -- Kārtošanas nosacījums adresācijas objekta nosaukumam, Varchar2 (516)
    DAT_SAK varchar(32),    -- Adresācijas objekta izveidošanas vai pirmreizējās reģistrācijas datums, ja nav zināms precīzs adresācijas objekta izveides datums., Date (dd.mm.yyyy)
    DAT_MOD varchar(32),    -- Datums un laiks, kad pēdējo reizi informācijas sistēmā tehniski modificēts ieraksts/ dati par adresācijas objektu, Date (dd.mm.yyyy hh:mm:ss)
    DAT_BEIG varchar(32),   -- Adresācijas objekta likvidācijas datums, ja adresācijas objekts beidza pastāvēt, Date (dd.mm.yyyy)
    ATRIB varchar(32)       -- ATVK kods intme, Varchar2 (32)
);


CREATE TABLE IF NOT EXISTS aw_vietu_centroidi (
    KODS int,               -- Adresācijas objekta kods, Number (9)
    TIPS_CD smallint,       -- Adresācijas objekta tipa kods, Number (3)
    NOSAUKUMS varchar(128), -- Adresācijas objekta nosaukums, Varchar2 (128)
    VKUR_CD int,            -- Tā adresācijas objekta kods, kuram hierarhiski pakļauts attiecīgais adresācijas objekts, Number (9)
    VKUR_TIPS smallint,     -- Tā adresācijas objekta tipa kods, kuram hierarhiski pakļauts attiecīgais adresācijas objekts, Number (3)
    STD varchar(256),       -- Adresācijas objekta pilnais adreses pierakst, Varchar2 (256)
    KOORD_X real,
    KOORD_Y real,
    DD_N real,
    DD_E real
);
