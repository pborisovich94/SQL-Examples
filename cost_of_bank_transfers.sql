-- FUNCTION: public.cost_of_bank_transfers(INTEGER, INTEGER, INTEGER, BOOLEAN, BOOLEAN, DATE, DATE, INTEGER) 
-- DROP FUNCTION public.cost_of_bank_transfers(INTEGER, INTEGER, INTEGER, BOOLEAN, BOOLEAN, DATE, DATE, INTEGER); 

CREATE OR REPLACE FUNCTION public.cost_of_bank_transfers(
    _id_balt_company        INTEGER, 
    _id_ins_company         INTEGER, 
    _id_country             INTEGER, 
    _full_commission        BOOLEAN, 
    _proportional_commisson BOOLEAN, 
    _date_begin             DATE, 
    _date_end               DATE, 
    _pdb_userid             INTEGER) 

    RETURNS TABLE(assistance_name TEXT, partner_name TEXT, country_name TEXT, transfer_date TEXT, cases_amount INTEGER, transfer_commission NUMERIC, transfer_currency TEXT, 
        bank_commission NUMERIC, bank_currency TEXT, commission_sum_eur INTEGER, transfer_sum TEXT, bank_sum TEXT)  

    LANGUAGE 'plpgsql' 
    COST 100 
    VOLATILE  
    ROWS 1000 

AS $BODY$ 

DECLARE 
    var_query TEXT; 

BEGIN
    CREATE LOCAL TEMP TABLE final_info( 
        assistance_name     TEXT, 
        partner_name        TEXT, 
        country_name        TEXT, 
        transfer_date       TEXT, 
        cases_amount        TEXT,  
        transfer_commission TEXT, 
        transfer_currency   TEXT,  
        bank_commission     TEXT,  
        bank_currency       TEXT,  
        commission_sum_eur  TEXT 
    ) ON COMMIT DROP; 

    CREATE LOCAL TEMP TABLE sum_info( 
        transfer_sum    TEXT, 
        bank_sum        TEXT 
    ) ON COMMIT DROP; 

    CREATE LOCAL TEMP TABLE final_info_2(
        assistance_name     TEXT, 
        partner_name        TEXT, 
        country_name        TEXT, 
        transfer_date       TEXT, 
        cases_amount        TEXT,  
        transfer_commission TEXT, 
        transfer_currency   TEXT,  
        bank_commission     TEXT,  
        bank_currency       TEXT,  
        commission_sum_eur  TEXT, 
        transfer_sum        TEXT, 
        bank_sum            TEXT 
    ) ON COMMIT DROP; 


    IF _full_commission = true THEN 
        var_query := 
            'WITH cte AS ( 
                SELECT
                    provider.id_provider AS id_med_center, 
                    provider.code_provider AS code_med_center,  
                    provider.rus_legal_name_provider AS name_med_center, 
                    ba_company.id_provider AS id_ref_ba_company, 
                    ba_company.rus_legal_name_provider AS name_ba_company, 
                    partner.id_provider AS id_ic, 
                    partner.rus_legal_name_provider AS name_ic, 
                    country.id_ref_country, 
                    country.rus_name_country AS name_country, 
                    td.id_doc AS id_bill, 
                    td.doc_date, 
                    COUNT(id_doc_detail) AS amount_cases 
                FROM finance.t_doc AS td 
                    LEFT JOIN finance.t_doc_detail AS tdd USING (id_doc) 
                    LEFT JOIN fregat.t_claim_case AS claim_case ON (tdd.id_case = claim_case.id_claim_case) 
                    LEFT JOIN fregat.t_claim AS claim ON (claim_case.id_claim = claim.id_claim) 
                    LEFT JOIN fregat.t_ref_country AS country ON (claim.id_ref_country_position = country.id_ref_country) 
                    LEFT JOIN fregat.t_provider AS ba_company ON (td.id_ref_ba_company = ba_company.id_provider) 
                    LEFT JOIN fregat.t_provider AS partner ON (claim.id_partner = partner.id_provider) 
                    LEFT JOIN fregat.t_provider AS provider ON (provider.id_provider = td.id_provider) 
                WHERE td.id_ref_operation_type = 5 
                    AND td.doc_type = 0 
                    AND td.dttmcl IS NULL 
                    AND tdd.dttmcl IS NULL 
                GROUP BY provider.id_provider, ba_company.id_provider, partner.id_provider, country.id_ref_country, id_bill 
                ORDER BY country 
            ), cte_2 AS ( 
                SELECT  
                    td.id_doc,  
                    COUNT(tdd.id_doc_detail) AS all_cases_amount 
                FROM finance.t_doc AS td 
                    LEFT JOIN finance.t_doc_detail AS tdd USING (id_doc) 
                GROUP BY td.id_doc) 

            INSERT INTO final_info 
            SELECT  
                cte.name_ba_company, 
                cte.name_ic, 
                cte.name_country, 
                cte.doc_date, 
                cte.amount_cases, 
                tc.transfer_commision_sum::numeric AS full_transfer,  
                trc_1.currency_code,  
                tc.bank_commision_sum::numeric AS full_bank,  
                trc_2.currency_code, 
                0 
            FROM cte  
                LEFT JOIN cte_2 ON cte_2.id_doc = cte.id_bill 
                LEFT JOIN finance.t_commissions AS tc USING (id_ref_ba_company, id_ref_country) 
                LEFT JOIN fregat.t_ref_currency AS trc_1 ON trc_1.id_ref_currency = tc.transfer_commision_crncy 
                LEFT JOIN fregat.t_ref_currency AS trc_2 ON trc_2.id_ref_currency = tc.bank_commision_crncy 
            WHERE cte.doc_date IS NOT NULL';


        IF _id_balt_company IS NOT NULL THEN 
            var_query := var_query || ' AND cte.id_ref_ba_company = ' || _id_balt_company; 
        END IF; 

        IF _id_ins_company IS NOT NULL THEN 
            var_query := var_query || ' AND cte.id_ic = ' || _id_ins_company; 
        END IF; 

        IF _id_country IS NOT NULL THEN 
            var_query := var_query || ' AND cte.id_ref_country = ' || _id_country; 
        END IF; 

        IF _date_begin IS NOT NULL AND _date_end IS NOT NULL THEN 
            var_query := var_query || ' AND cte.doc_date BETWEEN ''' || _date_begin::text || ''' AND ''' || _date_end::text || ''''; 
        END IF; 

        EXECUTE var_query; 


        INSERT INTO sum_info(transfer_sum) 
        SELECT sum(final_info.transfer_commission::numeric) || ' ' || final_info.transfer_currency 
        FROM final_info 
        GROUP BY final_info.transfer_currency; 

        INSERT INTO sum_info(bank_sum) 
        SELECT sum(final_info.bank_commission::numeric) || ' ' || final_info.bank_currency 
        FROM final_info 
        GROUP BY final_info.bank_currency; 

        INSERT INTO final_info_2(assistance_name,partner_name,country_name,transfer_date,cases_amount, transfer_commission,transfer_currency,bank_commission,bank_currency,commission_sum_eur) 
        SELECT * FROM final_info; 

        INSERT INTO final_info_2(transfer_sum, bank_sum) 
        SELECT string_agg(sum_info.transfer_sum, E'\r\n'), string_agg(sum_info.bank_sum, E'\r\n') FROM sum_info; 
    END IF; 


    IF _proportional_commisson = true THEN 
        var_query := 
            'WITH cte AS ( 
                SELECT  
                    provider.id_provider AS id_med_center, 
                    provider.code_provider AS code_med_center,  
                    provider.rus_legal_name_provider AS name_med_center, 
                    ba_company.id_provider AS id_ref_ba_company, 
                    ba_company.rus_legal_name_provider AS name_ba_company, 
                    partner.id_provider AS id_ic, 
                    partner.rus_legal_name_provider AS name_ic, 
                    country.id_ref_country, 
                    country.rus_name_country AS name_country, 
                    td.id_doc AS id_bill, 
                    td.doc_date, 
                    COUNT(id_doc_detail) AS amount_cases 
                FROM finance.t_doc AS td 
                    LEFT JOIN finance.t_doc_detail AS tdd USING (id_doc) 
                    LEFT JOIN fregat.t_claim_case AS claim_case ON (tdd.id_case = claim_case.id_claim_case) 
                    LEFT JOIN fregat.t_claim AS claim ON (claim_case.id_claim = claim.id_claim) 
                    LEFT JOIN fregat.t_ref_country AS country ON (claim.id_ref_country_position = country.id_ref_country) 
                    LEFT JOIN fregat.t_provider AS ba_company ON (td.id_ref_ba_company = ba_company.id_provider) 
                    LEFT JOIN fregat.t_provider AS partner ON (claim.id_partner = partner.id_provider) 
                    LEFT JOIN fregat.t_provider AS provider ON (provider.id_provider = td.id_provider) 
                WHERE td.id_ref_operation_type = 5 
                    AND td.doc_type = 0 
                    AND td.dttmcl IS NULL 
                    AND tdd.dttmcl IS NULL 
                GROUP BY provider.id_provider, ba_company.id_provider, partner.id_provider, country.id_ref_country, id_bill 
                ORDER BY country 
            ), cte_2 AS ( 
                SELECT  
                    td.id_doc,  
                    COUNT(tdd.id_doc_detail) AS all_cases_amount 
                FROM finance.t_doc AS td 
                    LEFT JOIN finance.t_doc_detail AS tdd USING (id_doc) 
                GROUP BY td.id_doc) 

            INSERT INTO final_info 
            SELECT  
                cte.name_ba_company,  
                cte.name_ic,  
                cte.name_country, 
                cte.doc_date, 
                cte.amount_cases, 
                round(tc.transfer_commision_sum::numeric * cte.amount_cases::numeric / cte_2.all_cases_amount::numeric, 1) AS proportional_transfer,  
                trc_1.currency_code,  
                round(tc.bank_commision_sum::numeric * cte.amount_cases::numeric / cte_2.all_cases_amount::numeric, 1) AS proportional_bank,  
                trc_2.currency_code, 
                0 
            FROM cte  
                LEFT JOIN cte_2 ON cte_2.id_doc = cte.id_bill 
                LEFT JOIN finance.t_commissions AS tc USING (id_ref_ba_company, id_ref_country) 
                LEFT JOIN fregat.t_ref_currency AS trc_1 ON trc_1.id_ref_currency = tc.transfer_commision_crncy 
                LEFT JOIN fregat.t_ref_currency AS trc_2 ON trc_2.id_ref_currency = tc.bank_commision_crncy 
            WHERE cte.doc_date IS NOT NULL'; 


        IF _id_balt_company IS NOT NULL THEN 
            var_query := var_query || ' AND cte.id_ref_ba_company = ' || _id_balt_company; 
        END IF; 

        IF _id_ins_company IS NOT NULL THEN 
            var_query := var_query || ' AND cte.id_ic = ' || _id_ins_company; 
        END IF; 

        IF _id_country IS NOT NULL THEN 
            var_query := var_query || ' AND cte.id_ref_country = ' || _id_country; 
        END IF; 

        IF _date_begin IS NOT NULL AND _date_end IS NOT NULL THEN 
            var_query := var_query || ' AND cte.doc_date BETWEEN ''' || _date_begin::text || ''' AND ''' || _date_end::text || ''''; 
        END IF; 

        EXECUTE var_query; 


        INSERT INTO sum_info(transfer_sum) 
        SELECT sum(final_info.transfer_commission::numeric) || ' ' || final_info.transfer_currency 
        FROM final_info 
        GROUP BY final_info.transfer_currency; 

        INSERT INTO sum_info(bank_sum) 
        SELECT sum(final_info.bank_commission::numeric) || ' ' || final_info.bank_currency 
        FROM final_info 
        GROUP BY final_info.bank_currency; 

        INSERT INTO final_info_2(assistance_name,partner_name,country_name,transfer_date,cases_amount, transfer_commission,transfer_currency,bank_commission,bank_currency,commission_sum_eur) 
        SELECT * FROM final_info; 

        INSERT INTO final_info_2(transfer_sum, bank_sum) 
        SELECT string_agg(sum_info.transfer_sum, E'\r\n'), string_agg(sum_info.bank_sum, E'\r\n') FROM sum_info; 
    END IF; 


    RETURN QUERY 
        SELECT 
            final_info_2.assistance_name, 
            final_info_2.partner_name, 
            final_info_2.country_name,  
            'Дата перевода: ' || to_char(final_info_2.transfer_date::timestamp, 'DD-MM-YYYY'), 
            final_info_2.cases_amount::integer,  
            final_info_2.transfer_commission::numeric,  
            final_info_2.transfer_currency,  
            final_info_2.bank_commission::numeric,  
            final_info_2.bank_currency,  
            final_info_2.commission_sum_eur::integer, 
            final_info_2.transfer_sum, 
            final_info_2.bank_sum 
        FROM final_info_2; 
END;
$BODY$; 