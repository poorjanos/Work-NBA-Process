/*  Data genereation pipeline for event stream in new business acquisition cycle */

--------------------------------------------------------------------------------
/* 
Generate event stream from manual workflow logs
    NOTE: must specify time scope
*/

DROP TABLE t_event_stream;
COMMIT;

CREATE TABLE t_event_stream
AS
     SELECT   a.f_ivk AS case_id,
              lpad(row_number() over ( order by a.f_ivk ), 6, 0) as event_id,
              f_int_begin AS event_begin,
              f_int_end AS event_end,
              CASE
                 WHEN f_oka = 'Szakmai guru' THEN 'specialist_help'
                 WHEN f_oka = 'Reponálás' THEN 'response_due'
                 WHEN f_oka = 'ujbelepok_elore' THEN 'vip_new_agents'
                 WHEN f_oka = 'kgfb2017_kampany' THEN '2017_tpml_campaigh'
                 WHEN f_oka = 'KIVKEZ' THEN 'exception_handling'
                 WHEN f_oka = 'Ébredés' THEN 'due_date_wakeup'
                 WHEN f_oka = 'Normál' THEN 'normal'
                 WHEN f_oka = 'Kapcsolódó' THEN 'missing_arrived'
                 ELSE 'unspecified'
              END
                 AS event_trigger,
              CASE
                 WHEN f_oka is null or f_oka not in ('Szakmai guru', 'Reponálás', 'ujbelepok_elore', 'kgfb2017_kampany', 'KIVKEZ', 'Ébredés', 'Normál', 'Kapcsolódó') then 'egyéb'
                 ELSE lower(f_oka)
              END
                 AS event_trigger_hu,
              CASE
                 WHEN (   a.f_alirattipusid BETWEEN 1896 AND 1930
                       OR a.f_alirattipusid BETWEEN 1944 AND 1947
                       OR a.f_alirattipusid IN ('1952', '2027', '2028', '2021'))
                 THEN
                    lower(kontakt.basic.get_alirattipusid_alirattipus (
                       a.f_alirattipusid
                    ))
                 ELSE
                    'egyéb iraton'
              END
                 AS event_name_hu,
              CASE
                 WHEN afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) =
                         'KÜT ügyfélkezelés indítása, általános, KÜT'
                 THEN
                    'cdb_prepared'
                 WHEN afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) LIKE
                         '%További ajánlati tevékenység szükséges tovább%'
                      OR afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) LIKE
                           '%Senior validálásra%'
                 THEN
                    'forwarded'
                 WHEN afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) LIKE
                         '%Szakmai segítséget kérek%'
                 THEN
                    'forwarded_need_help'
                 WHEN afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) LIKE
                         '%Várakoztatás szükséges%'
                      OR afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) LIKE
                           '%Nem zárható le/Reponálás funkció/Reponálás%'
                 THEN
                    'put_on_hold'
                 WHEN afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) LIKE
                         '%ötvényesítve%'
                 THEN
                    'finalized_contract'
                 WHEN afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) LIKE
                         '%lutasítva%'
                 THEN
                    'finalized_rejected'
                 WHEN afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) LIKE
                         '%átadás csoportvezetõnek%'
                 THEN
                    'forwarded_need_manager'
                 WHEN afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) LIKE
                         '%csoportvezetõi döntés%'
                 THEN
                    'manager_decision'
                 WHEN afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) LIKE
                         '%Nem indítható rögzítés%'
                 THEN
                    'system_error'
              END
                 AS event_result,
              CASE
                 WHEN afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) =
                         'KÜT ügyfélkezelés indítása, általános, KÜT'
                 THEN
                    'KUT'
                 WHEN afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) LIKE
                         '%További ajánlati tevékenység szükséges tovább%'
                      OR afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) LIKE
                           '%Senior validálásra%'
                 THEN
                    'tovabbad'
                 WHEN afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) LIKE
                         '%Szakmai segítséget kérek%'
                 THEN
                    'segitseg'
                 WHEN afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) LIKE
                         '%Várakoztatás szükséges%'
                      OR afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) LIKE
                           '%Nem zárható le/Reponálás funkció/Reponálás%'
                 THEN
                    'varakoztat'
                 WHEN afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) LIKE
                         '%ötvényesítve%'
                 THEN
                    'lezar_meneszt'
                 WHEN afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) LIKE
                         '%lutasítva%'
                 THEN
                    'lezar_elutasit'
                 WHEN afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) LIKE
                         '%átadás csoportvezetõnek%'
                 THEN
                    'csopoveznek'
                 WHEN afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) LIKE
                         '%csoportvezetõi döntés%'
                 THEN
                    'csopvez_dont'
                 WHEN afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) LIKE
                         '%Nem indítható rögzítés%'
                 THEN
                    'rendszer_hiba'
              END
                 AS event_result_hu,
              lower(afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid)) activity_string,
              lower(kontakt.basic.get_userid_kiscsoport (a.f_userid))  as workgroup,
              lower(kontakt.basic.get_userid_torzsszam (a.f_userid)) AS user_id,
              CASE
                 WHEN b.f_termcsop = 'CASCO' THEN 'casco'
                 WHEN b.f_termcsop = 'GÉP' THEN 'tpml'
                 WHEN b.f_termcsop = 'GÉPK' THEN 'tpmlse'
                 WHEN b.f_termcsop = 'LAK' THEN 'home'
                 WHEN b.f_termcsop = 'ÉLET' THEN 'life'
                 WHEN b.f_termcsop = 'TLP' THEN 'building'
              END
                 AS product_line,
              lower(b.f_termcsop) as product_line_hu,
              b.f_modkod AS product_id,
              CASE
                 WHEN b.f_kpi_kat = 'normál FE' THEN 'frontend'
                 WHEN b.f_kpi_kat = 'elektra' THEN 'elektra'
                 WHEN b.f_kpi_kat = 'e-nyilatkozat' THEN 'estatement'
                 WHEN b.f_kpi_kat = 'távértékesítés' THEN 'telesales'
                 WHEN b.f_kpi_kat = 'sima papír' THEN 'paper'
                 WHEN b.f_kpi_kat = 'mysigno' THEN 'mysigno'
              END
                 AS medium_type,
              lower(b.f_kpi_kat) as medium_type_hu
       FROM   afc.t_afc_wflog_lin2 a,
              kontakt.t_lean_alirattipus x,
              kontakt.t_ajanlat_attrib b
      WHERE       a.f_int_begin >= DATE '2018-01-01'
              AND (a.f_int_end - a.f_int_begin) * 1440 < 45
              AND (a.f_int_end - a.f_int_begin) * 86400 > 5
              AND afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) IS NOT NULL
              AND a.f_ivk = b.f_ivk(+)
              AND a.f_alirattipusid = x.f_alirattipusid
              AND UPPER (kontakt.basic.get_userid_login (a.f_userid)) NOT IN
                       ('MARKIB', 'SZERENCSEK')
              AND kontakt.basic.get_userid_kiscsoport (a.f_userid) IS NOT NULL
              AND x.f_lean_tip = 'AL'
              AND b.f_termcsop IS NOT NULL
              AND b.f_erkezes >= DATE '2017-01-01'
              AND f_erkezes IS NOT NULL
              AND b.f_kecs_pg = 'Feldolgozott'
              AND a.feladat <> 'LAKÁS átdolgozás'
              AND b.f_kecs <> 'Törölve'
   ORDER BY   a.f_ivk, a.f_int_begin;

COMMIT;

CREATE INDEX t_event_stream_idx
   ON t_event_stream (case_id, event_begin, event_end);

COMMIT;


--Delete proposals with incomplete taskresults

DELETE FROM   t_event_stream
      WHERE   case_id IN (SELECT   case_id
                               FROM   t_event_stream
                              WHERE   event_result IS NULL);

COMMIT;


--Delete proposals with outlier tasks

DELETE FROM   t_event_stream
      WHERE   case_id IN
                    (SELECT   case_id
                       FROM   t_event_stream
                      WHERE   event_name_hu IN
                                    ('casco senior kockázatelbírálás',
                                     'casco senior adatellenõrzés',
                                     'tlp senior kockázatelbírálás gyors',
                                     'gfb senior kockázatelbírálás',
                                     'tlp senior adatellenõrzés',
                                     'tlp adatrögzítés',
                                     'tlp papír adatellenõrzés',
                                     'egyéb iraton'));

COMMIT;


--delete unfinished proposals

DELETE FROM   t_event_stream a
      WHERE   case_id NOT IN
                    (SELECT   case_id
                       FROM   t_event_stream
                      WHERE   event_result IN
                                    ('finalized_contract',
                                     'finalized_rejected'));

COMMIT;


--delete tasks with duplicate ends
DELETE FROM t_event_stream where case_id in
(
SELECT   case_id
  FROM   (  SELECT   case_id, COUNT (case_id) AS cnt
              FROM   t_event_stream
             WHERE   event_result = 'finalized_contract'
          GROUP BY   case_id
          ORDER BY   COUNT (case_id) DESC)
 WHERE   cnt > 1
);
COMMIT;


DELETE FROM t_event_stream where case_id in
(
SELECT   case_id
  FROM   (  SELECT   case_id, COUNT (case_id) AS cnt
              FROM   t_event_stream
             WHERE   event_result = 'finalized_rejected'
          GROUP BY   case_id
          ORDER BY   COUNT (case_id) DESC)
 WHERE   cnt > 1
);
COMMIT;


--translate tasknames
ALTER TABLE t_event_stream
ADD(
event_name varchar2(50));
COMMIT;

UPDATE   t_event_stream a
   SET   event_name =
            (SELECT   lower(taskname_en)
               FROM   tasknames b
              WHERE   lower(a.event_name_hu) = lower(b.taskname));

COMMIT;


--Specifiy varakoztato
ALTER TABLE t_event_stream ADD
(put_on_hold_outcome varchar2(60),
put_on_hold_reason varchar2(60));
COMMIT;

--outcomes
DROP TABLE t_event_stream_put_on_hold_out;
COMMIT;

CREATE TABLE t_event_stream_put_on_hold_out
AS
   SELECT   case_id,
            event_begin,
            event_end,
            CASE
               WHEN event_result = 'put_on_hold'
                    AND activity_string like '%pkr%'
               THEN
                  'pkr'
               WHEN event_result = 'put_on_hold'
                    AND activity_string like '%email küldése/partner%'
               THEN
                  'email_partner'
                WHEN event_result = 'put_on_hold'
                    AND activity_string like '%email küldése/egyéb%'
               THEN
                  'email_other'
               WHEN event_result = 'put_on_hold'
                    AND activity_string like '%spoolsys%'
               THEN
                  'spool'
               WHEN event_result = 'put_on_hold'
                    AND activity_string like '%reponálás%'
               THEN
                  'repon'
               ELSE
                  'no_contact'
            END
               AS put_on_hold_outcome
     FROM   t_event_stream a
    WHERE   event_result = 'put_on_hold';

COMMIT;


DROP INDEX t_event_stream_outcome_idx;
CREATE INDEX t_event_stream_outcome_idx
   ON t_event_stream_put_on_hold_out (case_id, event_begin, event_end);

COMMIT;


--update outcome
UPDATE   t_event_stream a
   SET   put_on_hold_outcome =
            (SELECT   put_on_hold_outcome
               FROM   t_event_stream_put_on_hold_out b
              WHERE       a.case_id = b.case_id
                      AND a.event_begin = b.event_begin
                      AND a.event_end = b.event_end);



--reason
DROP TABLE t_event_stream_put_on_hold_rea;
COMMIT;

CREATE TABLE t_event_stream_put_on_hold_rea
AS
   SELECT   case_id,
            event_begin,
            event_end,
            CASE
               WHEN activity_string LIKE '%igényfelmérõre vár%'
               THEN
                  'missing_cl_survey'
               WHEN activity_string LIKE '%szemlére vár%'
               THEN
                  'missing_site_inspec'
               WHEN activity_string LIKE '%központi engedélyre vár%'
               THEN
                  'missing_permission'
               WHEN activity_string LIKE '%élõ elõzmény%'
               THEN
                  'unterminated_pred'
               WHEN activity_string LIKE '%kapcsolódó ajánlatra vár%'
               THEN
                  'missing_linked_prop'
               WHEN activity_string LIKE '%elõzmény nem díjrendezett%'
               THEN
                  'pending_premium_pred'
               WHEN activity_string LIKE '%rogramhib%'
               THEN
                  'system_error'
               WHEN activity_string LIKE '%portfóliós eseményre vár%'
               THEN
                  'missing_port_event'
               WHEN activity_string LIKE '%közterület felvitel%'
               THEN
                  'missing_postalcode'
               ELSE
                  'unspecified'
            END
               AS put_on_hold_reason
     FROM   t_event_stream a
    WHERE   event_result = 'put_on_hold';

COMMIT;


DROP INDEX t_event_stream_reason_idx;
CREATE INDEX t_event_stream_reason_idx
   ON t_event_stream_put_on_hold_rea (case_id,
            event_begin,
            event_end);

COMMIT;


--update reason

UPDATE   t_event_stream a
   SET   put_on_hold_reason =
            (SELECT   put_on_hold_reason
               FROM   t_event_stream_put_on_hold_rea b
              WHERE       a.case_id = b.case_id
                      AND a.event_begin = b.event_begin
                      AND a.event_end = b.event_end);

COMMIT;

--------------------------------------------------------------------------------
/* 
Generate case table from two segments
    autouw
    manualuw
    NOTE: must specify time scope
*/
DROP TABLE t_case;
COMMIT;

CREATE TABLE t_case
AS
   SELECT   vonalkod AS proposal_id,
            szerzazon AS contract_id,
            modkod AS product_code,
            CASE
               WHEN modtyp = 'Vagyon' THEN 'Home'
               WHEN modtyp = 'GFB' THEN 'TPML'
               WHEN modtyp = 'Casco' THEN 'Casco'
               WHEN modtyp = 'Life' THEN 'Life'
            END
               AS product_line,
            ugynokkod AS partner_code,
            ugynoknev AS partner_name,
            ktikod AS kti_code,
            ktinev AS kti_name,
            ertcsat AS sales_channel_code,
            CASE
               WHEN ertcsat IN ('O', 'U', 'DU') THEN 'Network'
               WHEN ertcsat IN ('B', 'SB', 'I', 'C') THEN 'Broker'
               ELSE 'Direct'
            END
               AS sales_channel,
            CASE
               WHEN papir_tipus = 1 THEN 'paper'
               WHEN papir_tipus = 2 THEN 'FE'
               WHEN papir_tipus = 3 THEN 'FE'
               WHEN papir_tipus = 4 THEN 'apotlo'
               WHEN papir_tipus = 5 THEN 'elektra'
               WHEN papir_tipus = 6 THEN 'elek'
               WHEN papir_tipus = 7 THEN 'mysigno'
               WHEN papir_tipus = 8 THEN 'tavert'
               WHEN papir_tipus = 9 THEN 'enyil'
            END
               AS medium_type,
            'I' AS autouw,
            alirdat,
            erkdat,
            szerzdat
     FROM   (SELECT   * FROM pss_201801_kieg@dl_peep
             UNION
             SELECT   * FROM pss_201802_kieg@dl_peep
             UNION
             SELECT   * FROM pss_201803_kieg@dl_peep
             UNION
             SELECT   * FROM pss_201804_kieg@dl_peep)
    WHERE       alirdat IS NOT NULL
            AND szerzdat IS NOT NULL
            AND kimenet = 'ABLAK központi menesztés sikeres'
   UNION
   SELECT   vonalkod AS proposal_id,
            szerzazon AS contract_id,
            modkod AS product_code,
            CASE
               WHEN modtyp = 'Vagyon' THEN 'Home'
               WHEN modtyp = 'GFB' THEN 'TPML'
               WHEN modtyp = 'Casco' THEN 'Casco'
               WHEN modtyp = 'Life' THEN 'Life'
            END
               AS product_line,
            ugynokkod AS partner_code,
            ugynoknev AS partner_name,
            ktikod AS kti_code,
            ktinev AS kti_name,
            ertcsat AS sales_channel_code,
            CASE
               WHEN ertcsat IN ('O', 'U', 'DU') THEN 'Network'
               WHEN ertcsat IN ('B', 'SB', 'I', 'C') THEN 'Broker'
               ELSE 'Direct'
            END
               AS sales_channel,
            CASE
               WHEN papir_tipus = 1 THEN 'paper'
               WHEN papir_tipus = 2 THEN 'FE'
               WHEN papir_tipus = 3 THEN 'FE'
               WHEN papir_tipus = 4 THEN 'apotlo'
               WHEN papir_tipus = 5 THEN 'elektra'
               WHEN papir_tipus = 6 THEN 'elek'
               WHEN papir_tipus = 7 THEN 'mysigno'
               WHEN papir_tipus = 8 THEN 'tavert'
               WHEN papir_tipus = 9 THEN 'enyil'
            END
               AS medium_type,
            'N' AS autouw,
            alirdat,
            erkdat,
            szerzdat
     FROM   (SELECT   * FROM pss_201801_kieg@dl_peep
             UNION
             SELECT   * FROM pss_201802_kieg@dl_peep
             UNION
             SELECT   * FROM pss_201803_kieg@dl_peep
             UNION
             SELECT   * FROM pss_201804_kieg@dl_peep) a
    WHERE   alirdat IS NOT NULL AND szerzdat IS NOT NULL
            AND (kimenet <> 'ABLAK központi menesztés sikeres'
                 OR kimenet IS NULL)
            AND EXISTS
                  (SELECT   1
                     FROM   t_event_stream es
                    WHERE   a.vonalkod = es.case_id
                            AND es.event_result_hu IN
                                     ('lezar_meneszt', 'lezar_elutasit'));

COMMIT;

--------------------------------------------------------------------------------
/* 
Generate premiums to cases
*/
DROP TABLE T_DIJ_HELPER_ABLAK;
COMMIT;


CREATE TABLE T_DIJ_HELPER_ABLAK as
SELECT   
a.proposal_id,
a.contract_id,
         MIN (f_dijbeido) AS dijbefizdat,
         MIN (f_banknap) AS dijerkdat,
         MIN (f_datum) AS dijkonyvdat
  FROM   t_case a, ab_t_dijtabla@dl_peep b
 WHERE   a.contract_id = b.f_szerz_azon AND a.product_line <> 'Life'
 GROUP BY a.proposal_id, a.contract_id;
COMMIT;


DROP TABLE T_DIJ_HELPER_FUFI;
COMMIT;

CREATE TABLE T_DIJ_HELPER_FUFI
AS
     SELECT   
     c.proposal_id,
     c.contract_id,
              MIN (b.payment_date) AS dijbefizdat,
              MIN (b.value_date) AS dijerkdat,
              MIN (a.application_date) AS dijkonyvdat
       FROM   fmoney_in_application@dl_peep a,
              (SELECT   DISTINCT money_in_idntfr,
                                 payment_mode,
                                 money_in_type,
                                 ifi_mozgaskod,
                                 payment_date,
                                 value_date
                 FROM   fmoney_in@dl_peep) b,
              t_case c
      WHERE       c.proposal_id = a.proposal_idntfr
              AND a.money_in_idntfr = b.money_in_idntfr
              AND ref_entity_type = 'Premium'
              AND application_status = 'normal'
              AND a.cntry_flg = 'HU'
              AND a.currncy_code = 'HUF'
              AND money_in_type IN ('propprem', 'reguprem')
              AND c.product_line = 'Life'
   GROUP BY   c.proposal_id, c.contract_id;
COMMIT;

DROP TABLE T_DIJ_HELPER;
COMMIT;

--Merge helpers
CREATE TABLE T_DIJ_HELPER
AS
SELECT * from T_DIJ_HELPER_ABLAK
UNION 
SELECT * from T_DIJ_HELPER_FUFI;
COMMIT;

-------------------------------------------------------------------------------
/* 
Generate events stream for autouw cases
*/
DROP TABLE t_events_autouw_base;
COMMIT;

CREATE TABLE t_events_autouw_base
AS
   SELECT   proposal_id as case_id,
            'signature' AS event_name,
            'alairas' AS event_name_hu,
            alirdat AS event_begin,
            alirdat AS event_end
     FROM   t_case
    WHERE   autouw = 'I'
   UNION
   SELECT   proposal_id as case_id,
            'arrival' AS event_name,
            'erkezes' AS event_name_hu,
            erkdat AS event_begin,
            erkdat AS event_end
     FROM   t_case
    WHERE   autouw = 'I'
   UNION
   SELECT   proposal_id as case_id,
            'autoUW finalized_contract' AS event_name,
            'menesztes_kpm' AS event_name_hu,
            erkdat AS event_begin,
            erkdat AS event_end
     FROM   t_case
    WHERE   autouw = 'I';
COMMIT;


DROP TABLE t_events_autouw;
COMMIT;

CREATE TABLE t_events_autouw
AS
select * from t_events_autouw_base
UNION
SELECT   distinct a.case_id,
            'premium_payment' AS event_name,
            'dijbefizetes' AS event_name_hu,
            b.dijbefizdat AS event_begin,
            b.dijbefizdat AS event_end
     FROM   t_events_autouw_base a, T_DIJ_HELPER b 
    WHERE   a.case_id = b.proposal_id
    UNION
SELECT   distinct a.case_id,
            'premium_arrival' AS event_name,
            'dijberkezes' AS event_name_hu,
            b.dijerkdat AS event_begin,
            b.dijerkdat AS event_end
     FROM   t_events_autouw_base a, T_DIJ_HELPER b 
    WHERE   a.case_id = b.proposal_id
     UNION
SELECT   distinct a.case_id,
            'premium_booking' AS event_name,
            'dijkonyveles' AS event_name_hu,
            b.dijkonyvdat AS event_begin,
            b.dijkonyvdat AS event_end
     FROM   t_events_autouw_base a, T_DIJ_HELPER b 
    WHERE   a.case_id = b.proposal_id;
COMMIT;


--------------------------------------------------------------------------------
/* 
Generate events stream for manual cases
*/
DROP TABLE t_events_manual_base;
COMMIT;

CREATE TABLE t_events_manual_base
AS
   SELECT   proposal_id as case_id,
            'signature' AS event_name,
            'alairas' AS event_name_hu,
            alirdat AS event_begin,
            alirdat AS event_end
     FROM   t_case
    WHERE   autouw = 'N'
   UNION
   SELECT   proposal_id as case_id,
            'arrival' AS event_name,
            'erkezes' AS event_name_hu,
            erkdat AS event_begin,
            erkdat AS event_end
     FROM   t_case
    WHERE   autouw = 'N';

COMMIT;

DROP TABLE t_events_prem;
COMMIT;

CREATE TABLE t_events_prem
AS
   SELECT   * FROM t_events_manual_base
   UNION
   SELECT   DISTINCT a.case_id,
                     'premium_payment' AS event_name,
                     'dijbefizetes' AS event_name_hu,
                     b.dijbefizdat AS event_begin,
                     b.dijbefizdat AS event_end
     FROM   t_events_manual_base a, T_DIJ_HELPER b
    WHERE   a.case_id = b.proposal_id
   UNION
   SELECT   DISTINCT a.case_id,
                     'premium_arrival' AS event_name,
                     'dijberkezes' AS event_name_hu,
                     b.dijerkdat AS event_begin,
                     b.dijerkdat AS event_end
     FROM   t_events_manual_base a, T_DIJ_HELPER b
    WHERE   a.case_id = b.proposal_id
   UNION
   SELECT   DISTINCT a.case_id,
                     'premium_booking' AS event_name,
                     'dijkonyveles' AS event_name_hu,
                     b.dijkonyvdat AS event_begin,
                     b.dijkonyvdat AS event_end
     FROM   t_events_manual_base a, T_DIJ_HELPER b
    WHERE   a.case_id = b.proposal_id;
    
    
    
/* Gen metadata for manual stream */
DROP TABLE t_event_meta;
COMMIT;

CREATE TABLE t_event_meta
AS
   SELECT   case_id,
            event_id,
            event_begin,
            event_end,
            event_name,
            case
            when event_name like '%failed-automat data proofing%' then 'data proofing auw_rework'
            when event_name not like '%failed-automat%' and event_name like '%data proofing%' then 'data proofing'
            when event_name like '%recording%' then 'data recording'
            when event_name like '%medical evaluation%' then 'medical evaluation'
            end as event_name_simple,
            event_result,
            event_trigger,
               event_trigger
            || '-'
            || event_result
            || '-'
            || put_on_hold_outcome
               AS event_meta,
            event_name_hu,
            case
            when event_name_hu like '%sikertelen kpm adatellenõrzés%' then 'kpm adatellenõrzés'
            when event_name_hu not like '%sikertelen kpm%' and  event_name_hu like '%adatellenõrzés%' then 'adatellenõrzés'
            when event_name_hu like '%rögz%' then 'adatrögzítés'
            when event_name_hu like '%kockázatelb%' then 'orvosi elbírálás'
            end as event_name_simple_hu,
            event_result_hu,
            event_trigger_hu,
               event_trigger_hu
            || '-'
            || event_result_hu
            || '-'
            || put_on_hold_outcome
               AS event_meta_hu,
            put_on_hold_outcome
     FROM   t_event_stream;

COMMIT;


/* Select closing events */
DROP TABLE t_closing_events;
COMMIT;

/* Formatted on 2018. 05. 25. 11:25:30 (QP5 v5.115.810.9015) */
CREATE TABLE t_closing_events
AS
   SELECT   DISTINCT
            case_id,
            event_id,
            event_name_simple || ' ' || event_result AS event_name,
            event_name_simple_hu || ' ' || event_result_hu AS event_name_hu,
            event_begin,
            event_end
     FROM   t_event_meta
    WHERE   event_result IN ('finalized_contract', 'finalized_rejected')
            AND case_id IN (SELECT   case_id FROM t_events_manual_base);
COMMIT;


/* Select first events when not closing*/
DROP TABLE t_first_events;
COMMIT;

/* Formatted on 2018. 05. 25. 11:25:38 (QP5 v5.115.810.9015) */
CREATE TABLE t_first_events
AS
   SELECT   DISTINCT
            case_id,
            FIRST_VALUE(event_id)
               OVER (PARTITION BY case_id
                     ORDER BY event_begin
                     ROWS UNBOUNDED PRECEDING)
               AS event_id,
            FIRST_VALUE(   event_name_simple
                        || ' '
                        || event_result
                        || ' '
                        || put_on_hold_outcome)
               OVER (PARTITION BY case_id
                     ORDER BY event_begin
                     ROWS UNBOUNDED PRECEDING)
               AS event_name,
            FIRST_VALUE(   event_name_simple_hu
                        || ' '
                        || event_result_hu
                        || ' '
                        || put_on_hold_outcome)
               OVER (PARTITION BY case_id
                     ORDER BY event_begin
                     ROWS UNBOUNDED PRECEDING)
               AS event_name_hu,
            FIRST_VALUE(event_begin)
               OVER (PARTITION BY case_id
                     ORDER BY event_begin
                     ROWS UNBOUNDED PRECEDING)
               AS event_begin,
            FIRST_VALUE(event_end)
               OVER (PARTITION BY case_id
                     ORDER BY event_begin
                     ROWS UNBOUNDED PRECEDING)
               AS event_end
     FROM   t_event_meta
    WHERE   event_id NOT IN (SELECT   event_id FROM t_closing_events)
            AND case_id IN (SELECT   case_id FROM t_events_manual_base);
COMMIT;


/* Select in-between events: either medical eval or outbound contact*/
DROP TABLE t_inbetween_events;
COMMIT;

/* Formatted on 2018. 05. 25. 11:38:39 (QP5 v5.115.810.9015) */
CREATE TABLE t_inbetween_events
AS
   SELECT   DISTINCT
            case_id,
            event_id,
               event_name_simple
            || ' '
            || event_result
            || ' '
            || put_on_hold_outcome
               AS event_name,
               event_name_simple_hu
            || ' '
            || event_result_hu
            || ' '
            || put_on_hold_outcome
               AS event_name_hu,
            event_begin,
            event_end
     FROM   t_event_meta
    WHERE       case_id IN (SELECT   case_id FROM t_events_manual_base)
            AND event_id NOT IN (SELECT   event_id FROM t_closing_events)
            AND event_id NOT IN (SELECT   event_id FROM t_first_events)
            AND (event_name_simple = 'medical evaluation'
                 OR (put_on_hold_outcome IS NOT NULL
                     AND put_on_hold_outcome <> 'no_contact'));
COMMIT;

/* Merge events */
DROP TABLE t_events_manual;
COMMIT;

CREATE TABLE t_events_manual
AS
   SELECT   * FROM t_events_prem
   UNION
   SELECT   to_char(case_id),
            event_name,
            event_name_hu,
            event_begin,
            event_end
     FROM   t_closing_events
   UNION
   SELECT   to_char(case_id),
            event_name,
            event_name_hu,
            event_begin,
            event_end
     FROM   t_first_events
   UNION
   SELECT   to_char(case_id),
            event_name,
            event_name_hu,
            event_begin,
            event_end
     FROM   t_inbetween_events;
COMMIT;


--------------------------------------------------------------------------------
/* Merge autouw and manual event streams */
DROP TABLE t_events;
COMMIT;

CREATE TABLE t_events
AS
   SELECT   case_id,
            event_name,
            event_name_hu,
            CASE
               WHEN event_name = 'autoUW finalized_contract'
               THEN
                  event_begin + (1 / 1440) --offset autouw with 1 min
               WHEN event_name = 'premium_payment'
               THEN
                  event_begin + (1 / 1440) --offset premium_payment with 1 min
               ELSE
                  event_begin
            END
               AS event_begin,
            CASE
               WHEN event_name = 'autoUW finalized_contract'
               THEN
                  event_end + (1 / 1440)
               WHEN event_name = 'premium_payment'
               THEN
                  event_end + (1 / 1440) --offset premium_payment with 1 min
               ELSE
                  event_end
            END
               AS event_end
     FROM   t_events_autouw
   UNION
   SELECT   * FROM t_events_manual;

COMMIT;

--------------------------------------------------------------------------------
/* Enhance with proposal meta data and create process mining input*/
DROP TABLE t_newbusiness_event_log;
COMMIT;

CREATE TABLE t_newbusiness_event_log
AS
   SELECT   a.*,
            b.product_code,
            b.product_line,
            b.partner_code,
            b.partner_name,
            b.kti_code,
            b.kti_name,
            b.sales_channel_code,
            b.sales_channel,
            b.medium_type,
            b.autouw,
            TRUNC (szerzdat, 'mm') AS contract_period
     FROM   t_events a, t_case b
    WHERE   a.case_id = b.proposal_id;
COMMIT;