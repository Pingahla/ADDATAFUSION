CREATE OR REPLACE PROCEDURE ADDATAFUSION.DM_ADS.SP_LOAD_FS_TIKTOK_ADS_AGE_GENDER()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
  var v_adgroup_id, v_adgroup_nm, v_advertiser_id, v_ad_id, v_ad_nm, v_ad_txt, v_age_dc, v_campaign_id, v_campaign_nm,
  v_gender_dc, v_dpa_target_audience_type_dc, v_stat_dttm, v_impressions_qty, v_conversion_qty, v_convertion_pct,
  v_mobile_app_id, v_promotion_type_dc, v_tiktok_app_id, v_tiktok_app_nm, v_click_qty, v_cost_per_conversion_amt,
  v_cost_pre_result_amt, v_cost_per_click_amt, v_cost_per_thousand_impressions_amt, v_click_through_pct,
  v_real_time_conversion_qty, v_real_time_conversion_pct, v_real_time_cost_per_conversion_amt,
  v_real_time_cost_per_result_amt, v_real_time_result_qty, v_real_time_result_pct, v_result_qty, v_result_pct, v_spend_amt;
    
  var v_version_count;
  var count = 0;
  
    try {
    var cur_age_gender = snowflake.execute( 
      {sqlText: `SELECT ADGROUP_ID, ADGROUP_NM, ADVERTISER_ID, AD_ID, AD_NM, AD_TXT, AGE_DC, CAMPAIGN_ID,
      CAMPAIGN_NM, GENDER_DC, DPA_TARGET_AUDIENCE_TYPE_DC, STAT_DTTM, IMPRESSIONS_QTY, CONVERSION_QTY, CONVERTION_PCT,
      MOBILE_APP_ID, PROMOTION_TYPE_DC, TIKTOK_APP_ID, TIKTOK_APP_NM, CLICK_QTY, COST_PER_CONVERSION_AMT,
      COST_PRE_RESULT_AMT, COST_PER_CLICK_AMT, COST_PER_THOUSAND_IMPRESSIONS_AMT, CLICK_THROUGH_PCT,
      REAL_TIME_CONVERSION_QTY, REAL_TIME_CONVERSION_PCT, REAL_TIME_COST_PER_CONVERSION_AMT,
      REAL_TIME_COST_PER_RESULT_AMT, REAL_TIME_RESULT_QTY, REAL_TIME_RESULT_PCT, RESULT_QTY, RESULT_PCT, SPEND_AMT
      FROM ADDATAFUSION.TIKTOK_ADS.VW_FS_ADS_INSIGHTS_AGE_AND_GENDER`}
    );
    
    while (cur_age_gender.next()) {
      v_adgroup_id=cur_age_gender.getColumnValue(1);
      v_adgroup_nm=cur_age_gender.getColumnValue(2);
      v_advertiser_id=cur_age_gender.getColumnValue(3);
      v_ad_id=cur_age_gender.getColumnValue(4);
      v_ad_nm=cur_age_gender.getColumnValue(5);
      v_ad_txt=cur_age_gender.getColumnValue(6);
      v_age_dc=cur_age_gender.getColumnValue(7);
      v_campaign_id=cur_age_gender.getColumnValue(8);
      v_campaign_nm=cur_age_gender.getColumnValue(9);
      v_gender_dc=cur_age_gender.getColumnValue(10);
      v_dpa_target_audience_type_dc=cur_age_gender.getColumnValue(11);
      v_stat_dttm=cur_age_gender.getColumnValue(12);
      v_impressions_qty=cur_age_gender.getColumnValue(13);
      v_conversion_qty=cur_age_gender.getColumnValue(14);
      v_convertion_pct=cur_age_gender.getColumnValue(15);
      v_mobile_app_id=cur_age_gender.getColumnValue(16);
      v_promotion_type_dc=cur_age_gender.getColumnValue(17);
      v_tiktok_app_id=cur_age_gender.getColumnValue(18);
      v_tiktok_app_nm=cur_age_gender.getColumnValue(19);
      v_click_qty=cur_age_gender.getColumnValue(20);
      v_cost_per_conversion_amt=cur_age_gender.getColumnValue(21);
      v_cost_pre_result_amt=cur_age_gender.getColumnValue(22);
      v_cost_per_click_amt=cur_age_gender.getColumnValue(23);
      v_cost_per_thousand_impressions_amt=cur_age_gender.getColumnValue(24);
      v_click_through_pct=cur_age_gender.getColumnValue(25);
      v_real_time_conversion_qty=cur_age_gender.getColumnValue(26);
      v_real_time_conversion_pct=cur_age_gender.getColumnValue(27);
      v_real_time_cost_per_conversion_amt=cur_age_gender.getColumnValue(28);
      v_real_time_cost_per_result_amt=cur_age_gender.getColumnValue(29);
      v_real_time_result_qty=cur_age_gender.getColumnValue(30);
      v_real_time_result_pct=cur_age_gender.getColumnValue(31);
      v_result_qty=cur_age_gender.getColumnValue(32);
      v_result_pct=cur_age_gender.getColumnValue(33);
      v_spend_amt=cur_age_gender.getColumnValue(34);
      
      // Find the previous version of the current record AQUI FALTA MODIFICAR
      var stmt = snowflake.createStatement({
        sqlText: `SELECT COUNT(1) FROM "ADDATAFUSION"."DM_ADS"."FS_TIKTOK_ADS_INSIGHTS_BY_AGE_AND_GENDER" WHERE ADGROUP_ID=?
        AND ADVERTISER_ID=? AND AD_ID=? AND AGE_DC=? AND STAT_DTTM=? AND CAMPAIGN_ID=? AND GENDER_DC=?`,
        binds: [v_adgroup_id, v_advertiser_id, v_ad_id, v_age_dc, v_stat_dttm, v_campaign_id, v_gender_dc]
      });
      var result = stmt.execute();
      if (result.next()) {
        v_version_count = result.getColumnValue(1);
      }
      
      if (v_version_count === 0) { // There is no a new record, then we insert a new record
        var stmt = snowflake.createStatement({
            sqlText: `INSERT INTO ADDATAFUSION.DM_ADS.FS_TIKTOK_ADS_INSIGHTS_BY_AGE_AND_GENDER (ADGROUP_ID, ADGROUP_NM, ADVERTISER_ID,
            AD_ID, AD_NM, AD_TXT, AGE_DC, CAMPAIGN_ID, CAMPAIGN_NM, GENDER_DC, DPA_TARGET_AUDIENCE_TYPE_DC, STAT_DTTM,
            IMPRESSIONS_QTY, CONVERSION_QTY, CONVERSION_PCT, MOBILE_APP_ID, PROMOTION_TYPE_DC, TIKTOK_APP_ID, TIKTOK_APP_NM,
            CLICK_QTY, COST_PER_CONVERSION_AMT, COST_PER_RESULT_AMT, COST_PER_CLICK_AMT, COST_PER_THOUSAND_IMPRESSIONS_AMT,
            CLICK_THROUGH_PCT, REAL_TIME_CONVERSION_QTY, REAL_TIME_CONVERSION_PCT, REAL_TIME_COST_PER_CONVERSION_AMT,
            REAL_TIME_COST_PER_RESULT_AMT, REAL_TIME_RESULT_QTY, REAL_TIME_RESULT_PCT, RESULT_QTY, RESULT_PCT, SPEND_AMT)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        binds: [v_adgroup_id, v_adgroup_nm, v_advertiser_id, v_ad_id, v_ad_nm, v_ad_txt, v_age_dc, v_campaign_id, v_campaign_nm,
                v_gender_dc, v_dpa_target_audience_type_dc, v_stat_dttm, v_impressions_qty, v_conversion_qty, v_convertion_pct,
                v_mobile_app_id, v_promotion_type_dc, v_tiktok_app_id, v_tiktok_app_nm, v_click_qty, v_cost_per_conversion_amt,
                v_cost_pre_result_amt, v_cost_per_click_amt, v_cost_per_thousand_impressions_amt, v_click_through_pct,
                v_real_time_conversion_qty, v_real_time_conversion_pct, v_real_time_cost_per_conversion_amt,
                v_real_time_cost_per_result_amt, v_real_time_result_qty, v_real_time_result_pct, v_result_qty,
                v_result_pct, v_spend_amt]
        });
        stmt.execute();
        count++;
      } 
    }
    
    return "Procedure SP_LOAD_FS_TIKTOK_ADS_AGE_GENDER() completed successfully, records added: " + count;
  } catch (err) {
    return "Error executing procedure: " + err.message;
  }
$$;