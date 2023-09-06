CREATE OR REPLACE PROCEDURE ADDATAFUSION.DM_ADS.SP_LOAD_CD2_CAMPAIGN()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
  var v_campaign_id,
  v_campaign_nm,
  v_status_dc,
  v_buying_type_dc,
  v_start_dttm,
  v_objective_dc,
  v_row_dc;
    try {
    var cur_campaign = snowflake.execute(
      {sqlText: `SELECT ROW_CHANGE_DC, CAMPAIGN_ID, CAMPAIGN_NM, STATUS_DC, BUYING_TYPE_DC, START_DTTM, OBJECTIVE_DC FROM ADDATAFUSION.FACEBOOK_ADS.VW_CHANGES_TO_CD2_CAMPAIGN`}
    );
    while (cur_campaign.next()) {
	  v_row_dc = cur_campaign.getColumnValue(1);
      v_campaign_id = cur_campaign.getColumnValue(2);
      v_campaign_nm = cur_campaign.getColumnValue(3);
	  v_status_dc = cur_campaign.getColumnValue(4);
	  v_buying_type_dc = cur_campaign.getColumnValue(5);
	  v_start_dttm = cur_campaign.getColumnValue(6);
	  v_objective_dc = cur_campaign.getColumnValue(7);
	 
	  if (v_row_dc === "UPDATE") { //there is a previous record and we need update it and insert the new one
        var stmt = snowflake.createStatement({
          sqlText: `UPDATE ADDATAFUSION.DM_ADS.CD2_CAMPAIGN SET EFF_TO_DTTM = CURRENT_TIMESTAMP(), CURR_REC_IND = 'N' WHERE CAMPAIGN_ID = :1 AND CURR_REC_IND = 'Y'`,
          binds: [v_campaign_id]
        });
        stmt.execute();
        var stmt = snowflake.createStatement({
          sqlText: `INSERT INTO ADDATAFUSION.DM_ADS.CD2_CAMPAIGN (CAMPAIGN_ID, CAMPAIGN_NM, STATUS_DC, BUYING_TYPE_DC, START_DTTM, OBJECTIVE_DC, CURR_REC_IND, EFF_FM_DTTM, EFF_TO_DTTM)
		  VALUES (:1, :2, :3, :4, :5, :6, 'Y', CURRENT_TIMESTAMP(), '9999-12-31 23:59:00.000')`,
          binds: [v_campaign_id, v_campaign_nm, v_status_dc, v_buying_type_dc, v_start_dttm, v_objective_dc]
        });
        stmt.execute();
      }
	  else if (v_row_dc === "APPEND") { // There is no previous record, we insert a new record
        var stmt = snowflake.createStatement({
          sqlText: `INSERT INTO ADDATAFUSION.DM_ADS.CD2_CAMPAIGN (CAMPAIGN_ID, CAMPAIGN_NM, STATUS_DC, BUYING_TYPE_DC, START_DTTM, OBJECTIVE_DC, CURR_REC_IND, EFF_FM_DTTM, EFF_TO_DTTM)
		  VALUES (:1, :2, :3, :4, :5, :6, 'Y', CURRENT_TIMESTAMP(), '9999-12-31 23:59:00.000')`,
          binds: [v_campaign_id, v_campaign_nm, v_status_dc, v_buying_type_dc, v_start_dttm, v_objective_dc]
        });
        stmt.execute();
      }
	  else if (v_row_dc === "FINAL") { // The record was removed from de Landing zone, and we need to deactivate it
        var stmt = snowflake.createStatement({
          sqlText: `ADDATAFUSION.DM_ADS.CD2_CAMPAIGN SET EFF_TO_DTTM = CURRENT_TIMESTAMP(), CURR_REC_IND = 'N' WHERE CAMPAIGN_ID = :1 AND CURR_REC_IND = 'Y'`,
          binds: [v_campaign_id]
        });
        stmt.execute();
      }
    }
    return "Procedure completed successfully.";
  } catch (err) {
    return "Error executing procedure: " + err.message;
  }
$$;