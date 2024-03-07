Intro 
--------

    This repo describes how to load VZD open data to sql db.

Requiremnts 
------------

    sqlite3 (tested on 3.43.2)
    zip 

Runing 
-------

    Execute bash script `./run.sh` script will download datasets and load to `vsd.db`. 
    When db is ready you can easily query data, for exmaple : 

    $ sqlite3 vzd.db "select address, deal_date, deal_amount from tg where address like 'Dzelzavas iela 99%'"
    Dzelzavas iela 99 - 30, Rīga, LV-1084|16.02.2012|19749,46
    . . .

Links
------

 - https://data.gov.lv/dati/lv/dataset/nekustama-ipasuma-tirgus-datu-bazes-atvertie-dati
