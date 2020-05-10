-- View: public.vw_scenario_steps 
-- DROP VIEW public.vw_scenario_steps; 

CREATE OR REPLACE VIEW public.vw_scenario_steps AS 

    WITH RECURSIVE tmp_tb(id_step, id_parent, step, code, name_text, script, id_ref_scenario, back_to_step) AS ( 
        SELECT tss.id_scenario_steps, 
            tss.id_parent, 
            (trss.code || ' '::text) || trss.name_text, 
            trss.code, 
            trss.name_text, 
            trssc.script_text, 
            tss.id_ref_scenario, 
            (trss_back.code || ' '::text) || trss_back.name_text AS back_to_step 
        FROM fregat.t_scenario_steps tss 
            LEFT JOIN fregat.t_ref_scenario_steps trss USING (id_ref_scenario_steps) 
            LEFT JOIN fregat.t_ref_scenario_scripts trssc USING (id_ref_scenario_scripts) 
            LEFT JOIN fregat.t_scenario_steps t_back_to_step ON tss.id_back_to_previous = t_back_to_step.id_scenario_steps 
            LEFT JOIN fregat.t_ref_scenario_steps trss_back ON t_back_to_step.id_ref_scenario_steps = trss_back.id_ref_scenario_steps 
        WHERE tss.id_parent IS NULL AND tss.dttmcl IS NULL 

        UNION ALL 

        SELECT tss_2.id_scenario_steps, 
            tss_2.id_parent, 
            (((((tmp_tb_1.code || ' '::text) || tmp_tb_1.name_text) || '/'::text) || trss_2.code) || ' '::text) || trss_2.name_text, 
            trss_2.code, 
            trss_2.name_text, 
            trssc.script_text, 
            tss_2.id_ref_scenario, 
            (trss_back.code || ' '::text) || trss_back.name_text AS back_to_step 
        FROM fregat.t_scenario_steps tss_2 
            LEFT JOIN fregat.t_ref_scenario_steps trss_2 USING (id_ref_scenario_steps) 
            LEFT JOIN fregat.t_ref_scenario_scripts trssc USING (id_ref_scenario_scripts) 
            LEFT JOIN fregat.t_scenario_steps t_back_to_step ON tss_2.id_back_to_previous = t_back_to_step.id_scenario_steps 
            LEFT JOIN fregat.t_ref_scenario_steps trss_back ON t_back_to_step.id_ref_scenario_steps = trss_back.id_ref_scenario_steps 
            JOIN tmp_tb tmp_tb_1 ON tmp_tb_1.id_step = tss_2.id_parent 
        WHERE tss_2.dttmcl IS NULL) 

    SELECT tmp_tb.id_step, 
        tmp_tb.id_parent, 
        tmp_tb.step, 
        tmp_tb.code, 
        tmp_tb.name_text, 
        tmp_tb.script, 
        tmp_tb.id_ref_scenario, 
        tmp_tb.back_to_step 
    FROM tmp_tb;