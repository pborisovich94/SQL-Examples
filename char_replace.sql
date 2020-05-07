-- FUNCTION: public.replace_rus_to_eng(TEXT) 
-- DROP FUNCTION publi.replace_rus_to_eng(TEXT); 

CREATE OR REPLACE FUNCTION public.replace_rus_to_eng(var_text TEXT) 
    RETURNS TABLE(cyrillic TEXT, latin TEXT)  

    LANGUAGE 'plpgsql' 
    COST 100 
    VOLATILE  
    ROWS 1000 

AS $BODY$ 

DECLARE
    r                   INTEGER; 
    latin_chars         TEXT[] := ARRAY['e', 't', 'o', 'p', 'a', 'h', 'k', 'x', 'c', 'b', 'm']; 
    cyrillic_chars      TEXT[] := ARRAY['е', 'т', 'о', 'р', 'а', 'н', 'к', 'х', 'с', 'в', 'м']; 
    substring_text      TEXT; 
    simbol_position     INTEGER; 
    on_cyrillic         TEXT := var_text; 
    on_latin            TEXT := var_text; 

BEGIN 
    var_text = LOWER(var_text); 

    FOR r IN 1..length(var_text) BY 1 
    LOOP 
        SELECT SUBSTRING(var_text, r, 1) INTO substring_text; 
        
        IF (substring_text = ANY(cyrillic_chars)) THEN 
            SELECT unnest((SELECT array_positions(cyrillic_chars, substring_text))) INTO simbol_position; 
            on_latin := REPLACE(var_text, cyrillic_chars[simbol_position], latin_chars[simbol_position]); 
        END IF; 

        IF (substring_text = ANY(latin_chars)) THEN 
            SELECT unnest((SELECT array_positions(cyrillic_chars, substring_text))) INTO simbol_position; 
            on_cyrillic := REPLACE(var_text, latin_chars[simbol_position], cyrillic_chars[simbol_position]); 
        END IF; 
    END LOOP; 

    RETURN QUERY 
        SELECT UPPER(on_cyrillic), UPPER(on_latin); 

END; 
$BODY$; 