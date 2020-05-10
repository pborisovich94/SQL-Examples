-- FUNCTION: public.possible_diagnos(TEXT) 
-- DROP FUNCTION public.possible_diagnos(TEXT); 

CREATE OR REPLACE FUNCTION public.possible_diagnos(var_in_json TEXT) 

    RETURNS JSON 

    LANGUAGE 'plpgsql' 
    COST 100 
    VOLATILE  

AS $BODY$ 

DECLARE 
    var_symptoms    JSON; 
    var_disease     JSON; 
    var_condition   TEXT; 
    r               RECORD; 
    var_sql         TEXT; 
    var_sql_res     NUMERIC; 
    var_sql_res_2   TEXT; 
    var_sql_res_3   NUMERIC; 
    var_sql_res_4   NUMERIC; 
    var_sql_res_5   TEXT; 
    var_pgn_res     INTEGER; 
    simptom         TEXT[]; 

BEGIN 
    SELECT array_agg(value::text) INTO simptom 
    FROM json_array_elements_text(((var_in_json::json)->>'simptoms')::json);

    IF (simptom IS NULL) THEN 
        SELECT COALESCE(json_agg( 
            json_build_object( 
                'id_ref_directory', id_ref_type_disease, 
                'name_directory', code_disease || ' ' || rus_name_disease, 
                'code_disease', code_disease,  
                'name_disease', rus_name_disease, 
                'boris_Rank', '', 
                'type_rank', ''
            ) 
        ), '[]') INTO var_disease 
        FROM fregat.t_ref_type_disease 
        WHERE use_in_first_status; 
    ELSE 
        CREATE LOCAL TEMP TABLE tmp_all_diag( 
            id_ref_type_disease INTEGER, 
            code_disease        TEXT, 
            name_disease        TEXT, 
            id_symptoms_disease INTEGER[], 
            all_symptoms        INTEGER[], 
            count_on_cc         INTEGER 
        ) ON COMMIT DROP; 

        CREATE LOCAL TEMP TABLE tmp_diag( 
            id_ref_type_disease         INTEGER, 
            code_disease                TEXT, 
            name_disease                TEXT, 
            id_symptoms_disease         INTEGER[], 
            all_symptoms                INTEGER[], 
            count_on_cc                 INTEGER, 
            sum_count_on_cc             INTEGER, 
            coincidence                 NUMERIC,
            text_count                  TEXT, 
            coincidence_in_disease      NUMERIC,
            coincidence_not_in_disease  NUMERIC,
            pgn_coefficient             INTEGER, 
            max_simptoms                INTEGER, 
            CONSTRAINT id_ref_type_disease_pk PRIMARY KEY (id_ref_type_disease) 
        ) ON COMMIT DROP; 


        INSERT INTO tmp_all_diag 
        SELECT DISTINCT id_ref_type_disease,
            trim(code_disease),
            only_name,
            (SELECT array_agg(id_symptoms_disease)
             FROM fregat.t_symptoms_disease  
                INNER JOIN fregat.t_ref_symptoms_disease USING(id_ref_symptoms_disease)  
             WHERE id_ref_type_disease = ANY (this_parent) 
                AND t_symptoms_disease.dttmcl IS NULL 
            ) AS id_symptoms_disease, 
            NULL::integer[],  
            count_on_cc
        FROM fregat.t_symptoms_disease 
            INNER JOIN fregat.t_ref_symptoms_disease USING(id_ref_symptoms_disease) 
            INNER JOIN fregat.vw_ref_type_disease USING(id_ref_type_disease) 
        ORDER BY id_ref_type_disease; 


        FOR r IN (SELECT * FROM tmp_all_diag)
        LOOP 
            UPDATE tmp_all_diag  
            SET all_symptoms = (SELECT array_agg(id_ref_symptoms_disease) FROM fregat.t_symptoms_disease WHERE id_symptoms_disease = ANY (r.id_symptoms_disease)) 
            WHERE id_ref_type_disease = r.id_ref_type_disease; 
        END LOOP; 


        FOR r IN (SELECT unnest(simptom::text[]) AS sss)
        LOOP  
            INSERT INTO tmp_diag  
            SELECT *  
            FROM tmp_all_diag  
            WHERE r.sss::integer = ANY (all_symptoms) 
            ON CONFLICT (id_ref_type_disease) DO NOTHING; 
        END LOOP; 


        CREATE LOCAL TEMP TABLE chosen_answers( 
            id_question integer,  
            id_answer integer 
        ) ON COMMIT DROP; 

        INSERT INTO chosen_answers 
        SELECT * 
        FROM json_to_recordset(((var_in_json::json)->>'questions')::json) AS x (id_question integer, id_answer integer); 


        FOR r IN (SELECT * FROM chosen_answers) 
        LOOP 
            DELETE FROM tmp_diag  
            WHERE id_ref_type_disease IN (SELECT id_ref_type_disease  
                                          FROM tmp_diag 
                                            LEFT JOIN fregat.t_mkb_question USING(id_ref_type_disease) 
                                          WHERE id_ref_symptoms_disease_question = r.id_question 
                                            AND id_ref_type_answer <> r.id_answer 
                                            AND t_mkb_question.dttmcl IS NULL); 
        END LOOP; 


        UPDATE tmp_diag SET max_simptoms = (SELECT max(array_length(all_symptoms, 1)) FROM tmp_diag); 
        UPDATE tmp_diag SET sum_count_on_cc = (SELECT sum(count_on_cc) FROM tmp_diag);

 
        FOR r IN (SELECT * FROM tmp_diag) 
        LOOP 
            SELECT  
            (100 / r.max_simptoms::double precision) * count(e)::double precision,
            count(e)::integer || ' / ' || r.max_simptoms::integer,
            (100 / array_length(simptom::integer[], 1)::integer) * count(e)::integer, 
            100 - (100 / array_length(simptom::integer[], 1)::integer) * count(e)::integer
            INTO var_sql_res, var_sql_res_2, var_sql_res_3, var_sql_res_4 
            FROM (SELECT unnest(simptom::integer[])
                INTERSECT
                SELECT unnest(r.all_symptoms)) AS dt(e); 

            SELECT sum(pgn_coefficient) INTO var_pgn_res 
            FROM fregat.t_symptoms_disease  
                INNER JOIN fregat.t_ref_symptoms_disease USING(id_ref_symptoms_disease)  
            WHERE id_symptoms_disease = ANY (r.id_symptoms_disease) 
                AND id_ref_symptoms_disease = ANY (simptom::integer[]); 

            UPDATE tmp_diag  
            SET coincidence = round(var_sql_res, 2),
                text_count = var_sql_res_2,
                coincidence_in_disease = round(var_sql_res_3, 2), 
                coincidence_not_in_disease = round(var_sql_res_4, 2), 
                pgn_coefficient = var_pgn_res 
            WHERE id_ref_type_disease = r.id_ref_type_disease; 
        END LOOP; 


        SELECT COALESCE(json_agg(  
            json_build_object( 
                'id_ref_directory', id_ref_type_disease, 
                'name_directory', code_disease || ' ' || name_disease, 
                'code_disease', code_disease,  
                'name_disease', name_disease, 
                'boris_Rank', ddd.rank, 
                'type_rank', tr
            )
        ), '[]') INTO var_disease 
        FROM (SELECT  
                id_ref_type_disease, 
                code_disease, 
                name_disease, 
                all_symptoms, 
                text_count, 
                (coincidence + coincidence_in_disease - coincidence_not_in_disease + COALESCE(pgn_coefficient, 0) + round((100 / sum_count_on_cc::numeric) * count_on_cc::numeric, 2)) AS rank, 
                'text_count -> ' || COALESCE(text_count, '')  || ' (' || COALESCE(coincidence::text, '') || ' %) ; ' || 
                'Send in dis -> ' || COALESCE(coincidence_in_disease::text, '') || '; ' || 
                'Send not dis -> ' || COALESCE(coincidence_not_in_disease::text, '') || '; ' || 
                'pgn_coefficient -> ' || COALESCE(pgn_coefficient::text, '') || '; ' || 
                round((100 / sum_count_on_cc::numeric) * count_on_cc::numeric, 2) AS tr 
             FROM tmp_diag 
             ORDER BY coincidence + coincidence_in_disease - coincidence_not_in_disease + COALESCE(pgn_coefficient, 0) + round((100 / sum_count_on_cc::numeric) * count_on_cc::numeric, 2) DESC) AS ddd; 
    END IF; 

    RETURN var_disease; 
END; 
$BODY$; 