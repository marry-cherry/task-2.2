create or replace procedure dm.refresh_loan_holiday_info()
language plpgsql
as $$
begin
    delete from dm.loan_holiday_info;

    insert into dm.loan_holiday_info (
        deal_rk,
        effective_from_date,
        effective_to_date,
        agreement_rk,
        client_rk,
        department_rk,
        product_rk,
        product_name,
        deal_type_cd,
        deal_start_date,
        deal_name,
        deal_number,
        deal_sum,
        loan_holiday_type_cd,
        loan_holiday_start_date,
        loan_holiday_finish_date,
        loan_holiday_fact_finish_date,
        loan_holiday_finish_flg,
        loan_holiday_last_possible_date
    )
    with deal as (
        select
            deal_rk,
            deal_num,
            deal_name,
            deal_sum,
            client_rk,
            agreement_rk,
            deal_start_date,
            department_rk,
            product_rk,
            deal_type_cd,
            effective_from_date,
            effective_to_date
        from rd.deal_info
    ),
    loan_holiday as (
        select
            deal_rk,
            loan_holiday_type_cd,
            loan_holiday_start_date,
            loan_holiday_finish_date,
            loan_holiday_fact_finish_date,
            loan_holiday_finish_flg,
            loan_holiday_last_possible_date,
            effective_from_date,
            effective_to_date
        from rd.loan_holiday
    ),
    product as (
        select
            product_rk,
            product_name,
            effective_from_date,
            effective_to_date
        from rd.product
    ),
    holiday_info as (
        select
            d.deal_rk,
            lh.effective_from_date,
            lh.effective_to_date,
            d.deal_num as deal_number,
            lh.loan_holiday_type_cd,
            lh.loan_holiday_start_date,
            lh.loan_holiday_finish_date,
            lh.loan_holiday_fact_finish_date,
            lh.loan_holiday_finish_flg,
            lh.loan_holiday_last_possible_date,
            d.deal_name,
            d.deal_sum,
            d.client_rk,
            d.agreement_rk,
            d.deal_start_date,
            d.department_rk,
            d.product_rk,
            p.product_name,
            d.deal_type_cd
        from deal d
        left join loan_holiday lh on d.deal_rk = lh.deal_rk
                                 and d.effective_from_date = lh.effective_from_date
        left join product p on p.product_rk = d.product_rk
                           and p.effective_from_date = d.effective_from_date
    )
    select
        deal_rk,
        effective_from_date,
        effective_to_date,
        agreement_rk,
        client_rk,
        department_rk,
        product_rk,
        product_name,
        deal_type_cd,
        deal_start_date,
        deal_name,
        deal_number,
        deal_sum,
        loan_holiday_type_cd,
        loan_holiday_start_date,
        loan_holiday_finish_date,
        loan_holiday_fact_finish_date,
        loan_holiday_finish_flg,
        loan_holiday_last_possible_date
    from holiday_info;
end;
$$;
