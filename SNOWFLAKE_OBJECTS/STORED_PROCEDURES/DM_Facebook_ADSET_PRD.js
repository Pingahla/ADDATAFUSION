CREATE OR REPLACE PROCEDURE ADDATAFUSION.DM_ADS.SP_LOAD_CD2_ADSET()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
  var v_adset_id, v_adset_nm, v_status_dc, v_daily_budget_amt, v_lifetime_budget_amt, 
  v_budget_remaining_amt, v_targeting_array, v_start_dttm, end_dttm, v_created_dttm, v_updated_dttm;
  var v_row_dc;

  try {
    var cur_adset = snowflake.execute(
      {sqlText: `SELECT ROW_CHANGE_DC, ADSET_ID, ADSET_NM, STATUS_DC, DAILY_BUDGET_AMT, LIFETIME_BUDGET_AMT, 
	  BUDGET_REMAINING_AMT, TARGETING_ARRAY, START_DTTM, END_DTTM, CREATED_DTTM, UPDATED_DTTM FROM ADDATAFUSION.FACEBOOK_ADS.VW_CHANGES_TO_CD2_ADSET`}
    );

    while (cur_adset.next()) {
	  v_row_dc = cur_adset.getColumnValue(1);
      v_adset_id = cur_adset.getColumnValue(2);
      v_adset_nm = cur_adset.getColumnValue(3);
	  v_status_dc = cur_adset.getColumnValue(4);
	  v_daily_budget_amt = cur_adset.getColumnValue(5);
	  v_lifetime_budget_amt = cur_adset.getColumnValue(6);
	  v_budget_remaining_amt = cur_adset.getColumnValue(7);
	  v_targeting_array = cur_adset.getColumnValue(8);
	  v_start_dttm = cur_adset.getColumnValue(9);
	  end_dttm = cur_adset.getColumnValue(10);
	  v_created_dttm = cur_adset.getColumnValue(11);
	  v_updated_dttm = cur_adset.getColumnValue(12);
      
	  
	  if (v_row_dc === "UPDATE") { //there is a previous record and we need update it and insert the new one
        var stmt = snowflake.createStatement({
          sqlText: `UPDATE ADDATAFUSION.DM_ADS.CD2_ADSET SET EFF_TO_DTTM = CURRENT_TIMESTAMP(), CURR_REC_IND = 'N' WHERE ADSET_ID = :1 AND CURR_REC_IND = 'Y'`,
          binds: [v_adset_id]
        });
        stmt.execute();
        var stmt = snowflake.createStatement({
          sqlText: `INSERT INTO ADDATAFUSION.DM_ADS.CD2_ADSET (ADSET_ID, ADSET_NM, STATUS_DC, DAILY_BUDGET_AMT, LIFETIME_BUDGET_AMT,
		  BUDGET_REMAINING_AMT, TARGETING_ARRAY, START_DTTM, END_DTTM, CREATED_DTTM, UPDATED_DTTM, CURR_REC_IND, EFF_FM_DTTM, EFF_TO_DTTM) 
		  VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, 'Y', CURRENT_TIMESTAMP(), '9999-12-31 23:59:00.000')`,
          binds: [v_adset_id, v_adset_nm, v_status_dc, v_daily_budget_amt, v_lifetime_budget_amt, v_budget_remaining_amt, 
		  v_targeting_array, v_start_dttm, end_dttm, v_created_dttm, v_updated_dttm]
        });
        stmt.execute();
      }
	  
	  else if (v_row_dc === "APPEND") { // There is no previous record, we insert a new record
        var stmt = snowflake.createStatement({
          sqlText: `INSERT INTO ADDATAFUSION.DM_ADS.CD2_ADSET (ADSET_ID, ADSET_NM, STATUS_DC, DAILY_BUDGET_AMT, LIFETIME_BUDGET_AMT,
		  BUDGET_REMAINING_AMT, TARGETING_ARRAY, START_DTTM, END_DTTM, CREATED_DTTM, UPDATED_DTTM, CURR_REC_IND, EFF_FM_DTTM, EFF_TO_DTTM) 
		  VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, 'Y', CURRENT_TIMESTAMP(), '9999-12-31 23:59:00.000')`,
          binds: [v_adset_id, v_adset_nm, v_status_dc, v_daily_budget_amt, v_lifetime_budget_amt, v_budget_remaining_amt, 
		  v_targeting_array, v_start_dttm, end_dttm, v_created_dttm, v_updated_dttm]
        });
        stmt.execute();
      }
	  
	  else if (v_row_dc === "FINAL") { // The record was removed from de Landing zone, and we need to deactivate it
        var stmt = snowflake.createStatement({
          sqlText: `UPDATE ADDATAFUSION.DM_ADS.CD2_ADSET SET EFF_TO_DTTM = CURRENT_TIMESTAMP(), CURR_REC_IND = 'N' WHERE ADSET_ID = :1 AND CURR_REC_IND = 'Y'`,
          binds: [v_adset_id]
        });
        stmt.execute();
      }

    }

    return "Procedure completed successfully.";
  } catch (err) {
    return "Error executing procedure: " + err.message;
  }
$$;
