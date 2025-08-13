Requiremnts 
------------

    sqlite3 (tested on 3.43.2)
    UnZip  (tested on 6.0)

Running 
--------

    Execute bash/zsh script `./download.sh` script will download datasets.

    Execute python `./python3 load_sqlite.py` script will load rows to `vsd.db`. 
    When db is ready you can easily query data, for example : 

    $ sqlite3 vzd.db "select address, deal_date, deal_amount from tg where address like 'Dzelzavas iela 99%'"
    Dzelzavas iela 99 - 30, Rīga, LV-1084|16.02.2012|19749,46
    . . .

Links
------

 - https://data.gov.lv/dati/lv/dataset/nekustama-ipasuma-tirgus-datu-bazes-atvertie-dati
 - https://www.vzd.gov.lv/lv/NITIS-datu-atversana?utm_source=https%3A%2F%2Fdata.gov.lv%2F
    https://www.vzd.gov.lv/lv/media/5029/download?attachment