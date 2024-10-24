Basic select to search for a given operator by name or ID

SELECT *
FROM operator
WHERE oper_id = '___' / name = '___';

Query, which selects the number of bussiness days from the given date:

SELECT COUNT(*)
FROM clearing_item
WHERE business_date >= '2023-01-01';


Query, which will select the operator and terminals:

SELECT *
FROM operator
WHERE term_id = '___' AND oper = '___';

Query, which selects all taps by terminal:

SELECT *
FROM tap
WHERE term_id = '____';

Query to compare operators by their parameters

operator_id IN ( 7, 61 )



SELECT *
FROM auth_tx
WHERE resp_code != 0 AND oper_id = 5
AND timestmp >= NOW() - INTERVAL 1 DAY;

Find clearing_id by tap_id:

SELECT clearing_id
FROM ticket_result
WHERE tap_id IN ('4b55d13557ef364aae7abd69afc2afaa5c0832b5d9ba7a350851bb9385b7a085','b1bd3645b5024fb252b106a1b5974aba1c11d8b90c46cf467bb9af79e53ab3ce');

SELECT cldi_id, clearing_day_id, business_date, paywindow_id, vs
FROM clearing_item
WHERE cldi_id IN ('455da00e-c57e-403a-9350-0ab2e88810d1','996c2b61-b063-4c6b-9b8e-4fd53185577a');

Find 10 last taps from tap_temp 

select server_dttm from tap_temp where oper_id = 4 order by server_dttm limit 10;

Vyhleda seznam terminalu a jejich posledni volani na COMM2
SELECT * FROM farecomm2.terminal where id in (
'M1DPOZ0210',
'M1DPOZ0265',
'M1DPOZ5173'
) 
Lisof the the temp failed taps

select b.code, a.token, a.terminal_id,a.masked_pan,a.tx_dttm,a.tx_transport_data,a.status_message,a.sent_count,a.sent_dttm,a.country_code from tx_to_send a left join operator b on a.operator_id = b.id where status="unknown";




Fertagus - monthly STP registration


select count(reg_id) from registration where merchant_code = "FERTAGUS" and created between "2024-08-01 00:00:01" and "2024-08-31 23:59:59";

ADVANCED



Query, which closes all open windows for the given operator:

UPDATE pay_window
SET stage = 'closed'
WHERE oper_id = '____' AND stage = 'open';



Query, which closes all open windows for the given operator and specific tokens

UPDATE pay_window
SET stage ='closed'
WHERE oper_id = '82' AND stage != 'open' AND token IN (
'118fa1fcb4fd76edf4d23f58c2faca5842691fa872f275cfea3a1bbaaf1e9097d9',
'11c1f4b01c61421355489aad97d81d7df578c2d2e4f06f83081d25e501f470c9de',
'111b5c50d463ad715ebf1cdf328fefbcf6d9f326cb40742acd8575acff8f61eca5',
'11a2e9cfec9ba682d1cfaa40669becf3202e82a7cc87a224931d17b522d366d77e',
'11a2ee2615cb4d201d58fedad8b53b4fd76ab0d664c3821537b85525acd979a1a5',
'118f5091f213ac758a6dd035234d82662021eb0c6b833e648aa70a4d3bb7181d3a');



Query, which updates the selected tokens and changes them to NULL:

UPDATE token
SET last_ok_auth = NULL
WHERE token IN (
'11360AFF3035A9ADDEE4CD5A1564EB467BD2E5AF51A0996641F370A5C0B881037A',
'11A96B115E1193D6F784CB9DBB0ADBF6D3E388022DFC0C0B8D83563549B79EB432',
'11146A192783BA96125E9F0B271405B2A231EE5C10B25731ADDCB89BE3F2C6AEC5'
);



Query that outputs the terminal id and the time the tap was entered on it between a specific time range, which will be sorted by time in descending ( DESC ) or ascending ( ASC ) order

SELECT *
FROM tap
WHERE term_id = '___'
AND server_dttm BETWEEN '2023-01-01' AND '2023-03-01'
ORDER BY server_dttm DESC/ASC;



Query to check the clearing status from a specific day

switch _ with number which days you want to select

SELECT *
FROM fare.clearing_day
WHERE clearing_stage =! 'Closed'
AND business_date BETWEEN DATE_FORMAT(NOW() - INTERVAL _ DAY, '%y-%m-%d') AND DATE_FORMAT(NOW() - INTERVAL _ DAY, '%y-%m-%d);

hledání clearing day s danym ID 


SELECT cd FROM ClearingDay as cd "
        + "WHERE cd.cldId LIKE :like "
        + "ORDER BY cd.cldId ASC 


SELECT DISTINCT token
FROM pay_window
WHERE token IN ( SELECT token FROM clearing_item WHERE clearing_day_id = '0071-0001-20230711' ) AND amount_settled = 0 AND oper_id = 71 AND stage = 'closed' AND open_dttm BETWEEN '2023-07-01 00:00:00' AND '2023-08-02 23:59:59'



Query, které nam zjistí pro daného operátora a daný časový interval všechny tokeny bez duplikací

WITH Atokens AS (
SELECT DISTINCT token
FROM stoplist_inc
WHERE oper_id = 4
AND dttm BETWEEN '2021-05-01 00:00:00' AND '2021-10-31 23:59:59'
AND type = 1
)
SELECT token
FROM Atokens
WHERE NOT EXISTS (
    SELECT 1
    FROM stoplist_inc
    WHERE oper_id = 4
      AND token = Atokens.token
      AND dttm > '2021-11-01 00:00:00'
      AND type = 2
);


zadání od zákazníka:
could you please start generating (if possible automatically, otherwise manually) the transaction report which will go to the SFTP everyday ?
we need to start following the everyday closing of the day. (clearing result, payment result etc)
please generate it since the busines date : 05.12.2023 and continue generate it all next days

query

select o.code as OperCode, a.term_id, t.masked_pan, a.operation, a.resp_code, a.resp_message, pw.vs, a.timestmp as DateTime, a.brand, a.rrn, a.amount, CAST(pw.open_dttm AS DATE) as BusinessDay
from auth_tx a left join fare.token t on t.token = a.token left join pay_window pw on a.vs = pw.vs left join operator o on a.oper_id = o.oper_id
where a.oper_id = '143' AND a.timestmp BETWEEN  '2023-12-05 00:00:00' AND '2023-12-05 23:59:59' order by a.timestmp desc


Problém s JMK, kdy se negenerovaly VS pro tokeny/okna. NEJDŮLEŽITĚJŠÍ udělat si původní zalohy originálních dat. Query níže vyhledá všechny tokeny, ktere jsou v danem clearingu/final a pokud splnují podmínku, že je token bez VS pro ten clearing a payment tak mu random vygeneruje VS o velikosti 10 znaků, který je generovaný random čísly a písmeny

UPDATE clearing_item AS ci1
    JOIN (
        SELECT token, SUBSTRING(MD5(RAND()) FROM 1 for 10) AS random_string
        FROM clearing_item
        WHERE clearing_day_id IN ('0054-0001-28240325', '0054-FINAL-28240325')
            AND vs is NULL
        GROUP BY token
        HAVING COUNT(*) > 1
    )    AS unique_tokens ON ci1.token = unique_tokens.token
SET ci1.vs = unique_tokens.random_string
WHERE ci1.vs is NULL;


Vyhledá napříč operátory duplicitní VS pro pay_window ( jestliže je pro jedno VS více tokenů ) viz. Problém s INNOBALTICA

SELECT pwid, open_dttm, vs, oper_id, brand_proc, stage 
FROM pay_window 
WHERE (vs, oper_id) IN (SELECT vs, oper_id FROM pay_window where open_dttm BETWEEN '2024-05-09 00:00:01' AND '2024-05-09 23:59:59' GROUP BY vs, oper_id HAVING COUNT(*) > 1) 
ORDER BY open_dttm desc, vs LIMIT 10000;


Query pro ljuibljanu. Chtěli vedět všechny zaznamy v debtu, které měly, ale úspěšný tap, ale neproplatily se

WITH tst AS (
    SELECT server_dttm, par, masked_pan, term_id, token, tapid
    FROM (
        SELECT server_dttm, par, masked_pan, term_id, token, tapid,  
            ROW_NUMBER() OVER (PARTITION BY token ORDER BY server_dttm ASC) AS rn 
        FROM tap 
        WHERE server_dttm BETWEEN '2023-11-25' AND '2024-03-31 23:59:59' 
        AND oper_id = 192
    ) AS subquery
    WHERE rn = 1
)
SELECT a.server_dttm AS cas_prvniho_tapu, a.token, a.par, a.masked_pan,b.amount_to_settle, c.resp_code, c.resp_message , a.term_id, c.uuid FROM tst a 
     INNER JOIN pay_window b ON a.tapid = b.open_tap 
     INNER JOIN auth_tx c ON a.token =b.token AND c.vs = b.vs 
     WHERE b.open_dttm BETWEEN '2023-11-25' AND '2024-03-31 23:59:59' 
     AND b.oper_id =192
     AND c.timestmp BETWEEN '2023-11-25' AND '2024-03-31 23:59:59' 
     AND c.oper_id =192 
     AND b.stage IN ('authDeclined', 'debtFinal', 'debt', 'debtSettled', 'noAuthDone')
     AND c.operation NOT IN ('PREAUTH', 'CARD_VERIFY') ;