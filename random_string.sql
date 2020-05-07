-- FUNCTION: public.random_string(INTEGER) 
-- DROP FUNCTION public.random_string(INTEGER);

CREATE OR REPLACE FUNCTION public.random_string(string_length INTEGER) 
    RETURNS TEXT 

    LANGUAGE 'plpgsql' 
    COST 100
    VOLATILE

AS $BODY$ 

DECLARE 
    chars   TEXT[]  := '{0,1,2,3,4,5,6,7,8,9,A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z}'; 
    result  TEXT    := ''; 
    r       INTEGER := 0; 

BEGIN 
    IF string_length <= 0 THEN 
        RAISE EXCEPTION 'Please enter a number that is greater than zero'; 
    END IF; 

    FOR r IN 1..string_length 
    LOOP 
        result := result || chars[1 + random() * (array_length(chars, 1) - 1)]; 
    END LOOP; 

    RETURN result; 

END; 
$BODY$; 