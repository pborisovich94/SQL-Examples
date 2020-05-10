-- FUNCTION: public.automatic_notify(INTEGER) 
-- DROP FUNCTION public.automatic_notify(INTEGER); 

 

CREATE OR REPLACE FUNCTION public.automatic_notify(var_id_claim INTEGER) 

    RETURNS TEXT 

    LANGUAGE 'plpgsql' 
    COST 100 
    VOLATILE  

AS $BODY$ 

DECLARE 
    var_country                 INTEGER;
    var_city                    INTEGER;
    var_partner                 INTEGER;
    var_treatment               INTEGER;
    var_sql                     TEXT;
    var_where                   TEXT;
    r                           RECORD;
    var_id_claim_case_comment   INTEGER;

BEGIN 
    SELECT id_ref_country_position, id_ref_city_position, id_partner, id_ref_catalog_type_treatment  
    INTO var_country, var_city, var_partner, var_treatment 
    FROM fregat.t_claim  
    WHERE id_claim = var_id_claim; 


    var_sql = 
        'WITH cte AS ( 
            SELECT  
                id_automatic_notify,  
                mail_to,  
                mail_cc, 
                (SELECT array_agg(id_ref_country)  
                 FROM fregat.t_automatic_notify_country AS ancc  
                 WHERE ancc.id_automatic_notify = an.id_automatic_notify  
                    AND dttmcl IS NULL) AS country_array,  
                (SELECT array_agg(COALESCE(id_ref_city,-1))  
                 FROM fregat.t_automatic_notify_country AS anccity  
                 WHERE anccity.id_automatic_notify = an.id_automatic_notify  
                    AND dttmcl IS NULL) AS city_array, 
                (SELECT array_agg(id_partner)  
                 FROM fregat.t_automatic_notify_insurance AS ani  
                 WHERE ani.id_automatic_notify = an.id_automatic_notify  
                    AND dttmcl IS NULL) AS insurance_array, 
                (SELECT array_agg(id_ref_catalog)  
                 FROM fregat.t_automatic_notify_treatment AS ant  
                 WHERE ant.id_automatic_notify = an.id_automatic_notify  
                    AND dttmcl IS NULL) AS treatment_array 
            FROM fregat.t_automatic_notify as an 
            WHERE is_work = true 
                AND dttmcl IS NULL 
            GROUP BY id_automatic_notify, mail_to, mail_cc) 

        SELECT * FROM cte'; 


    var_where = CASE WHEN var_country IS NOT NULL THEN var_country || ' = ANY (COALESCE(country_array, (''{' || var_country || '}'')::integer[]))' END; 
    var_where = var_where || COALESCE(' AND ' || CASE WHEN var_city IS NOT NULL THEN var_city || ' = ANY (COALESCE(city_array, (''{' || var_city || '}'')::integer[]))' END, ''); 
    var_where = var_where || COALESCE(' AND ' || CASE WHEN var_partner IS NOT NULL THEN var_partner || ' = ANY (COALESCE(insurance_array, (''{' || var_partner || '}'')::integer[]))' END, ''); 
    var_where = var_where || COALESCE(' AND ' || CASE WHEN var_treatment IS NOT NULL THEN var_treatment || ' = ANY (COALESCE(treatment_array, (''{' || var_treatment || '}'')::integer[]))' END, ''); 
    var_where = var_where || ' AND TRUE = TRUE'; 


    SELECT id_claim_case_comment 
    INTO var_id_claim_case_comment 
    FROM fregat.t_claim  
        LEFT JOIN fregat.t_claim_case USING(id_claim) 
        LEFT JOIN fregat.t_claim_case_comment USING(id_claim_case) 
    WHERE id_claim = var_id_claim 
        AND id_ref_type_comment = 88; 


    FOR r IN EXECUTE (var_sql  || ' WHERE ' || var_where) 
    LOOP 
        INSERT INTO abc.t_mail_out_queue(id_user, mail_source_schema, mail_source_table, mail_source_row_id, mail_status, mail_to, mail_cc, mail_sender_name,  
        mail_sender_address, mail_subject, mail_body_type, mail_body) 
        SELECT 305, 1, 't_claim_case_comment', var_id_claim_case_comment, 0, r.mail_to, r.mail_cc, (SELECT f_get_username FROM abc.f_get_username(305, 2)),  
        'claim@calltravel.eu', 'Дела', 0, 'Заявка:'; 
    END LOOP; 

    RETURN 'Письмо отправлено успешно'; 
END; 
$BODY$; 