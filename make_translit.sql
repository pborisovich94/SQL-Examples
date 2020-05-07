-- FUNCTION: public.return_translit(TEXT) 
-- DROP FUNCTION public.return_translit(TEXT); 

CREATE OR REPLACE FUNCTION public.return_translit(var_input TEXT) 
    RETURNS TEXT 

    LANGUAGE 'plpgsql' 
    COST 100
    VOLATILE

AS $BODY$ 

DECLARE 
    cyrillic_chars      TEXT[] := array['а', 'б', 'в', 'г', 'д', 'е', 'ё', 'ж', 'з', 'и', 'й', 'к', 'л', 'м', 'н', 'о', 'п', 'р', 'с', 'т', 'у', 'ф', 'х', 'ц', 'ч', 'ш', 'щ', 'ъ', 'ы', 'ь', 'э', 'ю', 'я', ' ', ',', ';']; 
    latin_chars         TEXT[] := array['a', 'b', 'v', 'g', 'd', 'e', 'yo', 'zh', 'z', 'i', 'y', 'k', 'l', 'm', 'n', 'o', 'p', 'r', 's', 't', 'u', 'f', 'h', 'ts', 'ch', 'sh', 'sch', '', 'i', '', 'e', 'yu', 'ya', '_', '_', '_']; 
    var_result          TEXT := LOWER(var_input); 
    var_array_length    INTEGER; 

BEGIN 
    var_array_length := array_length(cyrillic_chars, 1); 

    FOR i IN 1..var_array_length 
    LOOP 
        var_result := REPLACE(var_result, cyrillic_chars[i], latin_chars[i]); 
    END LOOP; 

    RETURN var_result; 

END; 
$BODY$; 