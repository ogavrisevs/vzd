Requiremnts 
------------

    sqlite3 (tested on 3.43.2)
    UnZip  (tested on 6.0)

Running 
--------

    Execute bash script `./run.sh` script will download datasets and load to `vsd.db`. 
    When db is ready you can easily query data, for example : 

    $ sqlite3 vzd.db "select address, deal_date, deal_amount from tg where address like 'Dzelzavas iela 99%'"
    Dzelzavas iela 99 - 30, Rīga, LV-1084|16.02.2012|19749,46
    . . .

Links
------

 - https://data.gov.lv/dati/lv/dataset/nekustama-ipasuma-tirgus-datu-bazes-atvertie-dati
 - https://data.gov.lv/dati/lv/dataset/valsts-adresu-registra-informacijas-sistemas-atvertie-dati