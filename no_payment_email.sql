-- FUNCTION: public.no_payment_bill(INTEGER, INTEGER) 
-- DROP FUNCTION public.no_payment_bill(INTEGER, INTEGER); 

CREATE OR REPLACE FUNCTION public.no_payment_bill(
    _pdb_userid         INTEGER, 
    "308.id_partner"    INTEGER) 

    RETURNS TEXT 

    LANGUAGE 'plpgsql' 
    COST 100 
    VOLATILE  

AS $BODY$ 

DECLARE 
    var_new_rec     INTEGER; 
    var_signature   TEXT; 

BEGIN 
    var_signature := (SELECT * FROM abc.f_mail_signature(_pdb_userid, 'ru')); 

    WITH cte AS ( 
        SELECT 'Уважаемые Коллеги! Настоящим письмом уведомляем Вас об имеющейся задолженности ' || rus_legal_name_provider || ' перед ' || ba_name || ' сроком более ' || vw_doc.bill_payment_days || ' дней по следующим счетам:' || chr(10) AS company, 
            ('№' || ' ' || vw_doc.doc_nr || ' ' || 'от' || ' ' || vw_doc.doc_date::date || '  -  ' || COALESCE(vw_doc.sum_refund, '0.00') || ' ' || COALESCE(refund_crncy, fee_crncy) || ' / ' || COALESCE(vw_doc.sum_fee, '0.00') || ' ' ||  
            COALESCE(fee_crncy, refund_crncy) || ' (Возмещение / Вознаграждение).') AS bill, vw_doc.email, vw_doc.id_contract, COALESCE(refund_crncy, ' ') AS crncy_r, COALESCE(fee_crncy, ' ') AS crncy_f, vw_doc.sum_refund, vw_doc.sum_fee, 
            'В соответствии с условиями имеющегося договора №' || vw_doc.contract_nr || ' от ' || COALESCE(contract_date::text, ' ') || ', просим Вас погасить накопленную задолженность в течении 14 дней с момента получения данного письма.' AS ending 
        FROM finance.vw_doc 
            LEFT JOIN finance.t_doc ON vw_doc.id_doc = t_doc.id_doc_paid 
            INNER JOIN fregat.t_provider ON t_provider.id_provider = vw_doc.id_provider 
            LEFT JOIN finance.t_contract ON vw_doc.id_contract = t_contract.id_contract 
        WHERE (vw_doc.id_provider = "308.id_partner")  
            AND (vw_doc.dttmcl IS NULL)  
            AND (vw_doc.doc_type = 0 AND ((id_refund_type = 1 OR id_fee_type = 1) OR (id_refund_type IS NULL OR (id_fee_type IS NULL AND id_refund_type <> 7)))) 
            AND t_doc.id_doc IS NULL 
            AND (vw_doc.doc_date + vw_doc.bill_payment_days * interval '1 day') < now() 
            AND vw_doc.bill_payment_days IS NOT NULL 
            AND vw_doc.email IS NOT NULL  

    ), cte_2 AS ( 
        SELECT company, string_agg (bill, E'\r\n') as bills,
        COUNT(bill) || ' счет(ов) на общую сумму: ' || COALESCE(SUM(sum_refund), '0.00') || ' ' || crncy_r || ' (Возмещение) ' || COALESCE(SUM(sum_fee), '0.00') || ' ' || crncy_f || ' (Вознаграждение).' AS s_f, email, id_contract, ending 
        FROM cte 
        GROUP BY company, email, id_contract, ending, crncy_r, crncy_f  
    ), cte_3 AS ( 
        SELECT company, string_agg(bills || E'\r\n' || s_f || E'\r\n', E'\r\n') AS bills, email, id_contract, ending 
        FROM cte_2 
        GROUP BY company, email, id_contract, ending 
    ), cte_4 AS( 
        INSERT INTO abc.t_mail_out_queue(id_user, mail_source_schema, mail_source_table, mail_source_row_id, mail_status, mail_to, mail_sender_name, mail_sender_address, mail_subject, mail_body_type, mail_body) 
        SELECT _pdb_userid, 0, 'f_return_no_payment_bill', "308.id_partner", 0, email, (SELECT COALESCE((SELECT f_get_username FROM  abc.f_get_username(_pdb_userid, 2)),'Оператор №' || _pdb_userid)), 
            'sindikat@calltravel.eu', 'Просроченые счета', 0, string_agg(company ||  E'\r\n' || bills || E'\r\n' || E'\r\n' || ending || var_signature, E'\r\n' || E'\r\n')  
        FROM cte_3 
        GROUP BY company, email, ending, bills) 

    SELECT count(*) INTO var_new_rec FROM cte_4; 

    IF var_new_rec = 0 THEN 
        RETURN 'Письмо не отправлено'; 
    ELSE 
        RETURN 'Письмо отправлено успешно'; 
    END IF; 
END; 
$BODY$; 